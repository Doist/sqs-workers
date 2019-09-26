import logging
import warnings
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

import attr

from sqs_workers import DEFAULT_BACKOFF, codecs
from sqs_workers.async_task import AsyncTask
from sqs_workers.backoff_policies import BackoffPolicy
from sqs_workers.core import BatchProcessingResult, get_job_name
from sqs_workers.processors import DEFAULT_CONTEXT_VAR, Processor
from sqs_workers.shutdown_policies import NEVER_SHUTDOWN

DEFAULT_MESSAGE_GROUP_ID = "default"

if TYPE_CHECKING:
    from sqs_workers import SQSEnv

logger = logging.getLogger(__name__)


@attr.s
class GenericQueue(object):
    env = attr.ib(repr=False)  # type: SQSEnv
    name = attr.ib()  # type: str
    backoff_policy = attr.ib(default=DEFAULT_BACKOFF)  # type: BackoffPolicy
    _queue = attr.ib(repr=False, default=None)

    @classmethod
    def maker(cls, **kwargs):
        return partial(cls, **kwargs)

    def process_queue(self, shutdown_policy=NEVER_SHUTDOWN, wait_second=10):
        """
        Run worker to process one queue in the infinite loop
        """
        logger.debug(
            "Start processing queue {}".format(self.name),
            extra={
                "queue_name": self.name,
                "wait_seconds": wait_second,
                "shutdown_policy": repr(shutdown_policy),
            },
        )
        while True:
            result = self.process_batch(wait_seconds=wait_second)
            shutdown_policy.update_state(result)
            if shutdown_policy.need_shutdown():
                logger.debug(
                    "Stop processing queue {}".format(self.name),
                    extra={
                        "queue_name": self.name,
                        "wait_seconds": wait_second,
                        "shutdown_policy": repr(shutdown_policy),
                    },
                )
                break

    def process_batch(self, wait_seconds=0):
        # type: (int) -> BatchProcessingResult
        """
        Process a batch of messages from the queue (10 messages at most), return
        the number of successfully processed messages, and exit
        """
        queue = self.get_queue()
        messages = self.get_raw_messages(wait_seconds)
        result = BatchProcessingResult(self.name)

        for message in messages:
            success = self.process_message(message)
            result.update_with_message(message, success)
            if success:
                entry = {
                    "Id": message.message_id,
                    "ReceiptHandle": message.receipt_handle,
                }
                queue.delete_messages(Entries=[entry])
            else:
                timeout = self.backoff_policy.get_visibility_timeout(message)
                message.change_visibility(VisibilityTimeout=timeout)
        return result

    def process_message(self, message):
        # type: (Any) -> bool
        """
        Process single message.

        Return True if processing went successful
        """
        raise NotImplementedError()

    def get_raw_messages(self, wait_seconds):
        """
        Return raw messages from the queue, addressed by its name
        """
        kwargs = {
            "WaitTimeSeconds": wait_seconds,
            "MaxNumberOfMessages": 10,
            "MessageAttributeNames": ["All"],
            "AttributeNames": ["All"],
        }
        queue = self.get_queue()
        return queue.receive_messages(**kwargs)

    def drain_queue(self, wait_seconds=0):
        """
        Delete all messages from the queue without calling purge().
        """
        queue = self.get_queue()
        deleted_count = 0
        while True:
            messages = self.get_raw_messages(wait_seconds)
            if not messages:
                break
            entries = [
                {"Id": msg.message_id, "ReceiptHandle": msg.receipt_handle}
                for msg in messages
            ]
            queue.delete_messages(Entries=entries)
            deleted_count += len(messages)
        return deleted_count

    def get_sqs_queue_name(self):
        """
        Take "high-level" (user-visible) queue name and return SQS
        ("low level") name by simply prefixing it. Used to create namespaces
        for different environments (development, staging, production, etc)
        """
        return self.env.get_sqs_queue_name(self.name)

    def get_queue(self):
        """
        Helper function to return queue object.
        """
        if self._queue is None:
            self._queue = self.env.sqs_resource.get_queue_by_name(
                QueueName=self.get_sqs_queue_name()
            )
        return self._queue


@attr.s
class RawQueue(GenericQueue):
    processor = attr.ib(default=None)  # type: Optional[Callable]

    def raw_processor(self):
        """
        Decorator to assign processor to handle jobs from the raw queue

        Usage example:

        cron = sqs.queue('cron')

        @cron.raw_processor()
        def process(message):
            print(message.body)
        """

        def func(processor):
            return self.connect_raw_processor(processor)

        return func

    def connect_raw_processor(self, processor):
        """
        Assign raw processor (a function) to handle jobs.
        """
        extra = {
            "queue_name": self.name,
            "processor_name": processor.__module__ + "." + processor.__name__,
        }
        logger.debug(
            "Connect {queue_name} to " "processor {processor_name}".format(**extra),
            extra=extra,
        )
        self.processor = processor

    def add_raw_job(
        self,
        message_body,  # type: str
        delay_seconds=0,  # type: int
        deduplication_id=None,  # type: str
        group_id=DEFAULT_MESSAGE_GROUP_ID,  # type: str
    ):
        """
        Add raw message to the queue
        """

        kwargs = {
            "MessageBody": message_body,
            "DelaySeconds": delay_seconds,
            "MessageAttributes": {},
        }
        if self.name.endswith(".fifo"):
            # if queue name ends with .fifo, then according to the AWS specs,
            # it's a FIFO queue, and requires group_id.
            # Otherwise group_id can be set to None
            if group_id is None:
                group_id = DEFAULT_MESSAGE_GROUP_ID
            if deduplication_id is not None:
                kwargs["MessageDeduplicationId"] = str(deduplication_id)
            if group_id is not None:
                kwargs["MessageGroupId"] = str(group_id)

        ret = self.get_queue().send_message(**kwargs)
        return ret["MessageId"]

    def process_message(self, message):
        # type: (Any) -> bool
        """
        Process single message.

        Return True if processing went successful
        """
        extra = {"message_id": message.message_id, "queue_name": self.name}
        if not self.processor:
            logger.warning(
                "No processor set for {queue_name}".format(**extra), extra=extra
            )

        logger.debug("Process {queue_name}.{message_id}".format(**extra), extra=extra)

        try:
            self.processor(message)
        except Exception:
            logger.exception(
                "Error while processing {queue_name}.{message_id}".format(**extra),
                extra=extra,
            )
            return False
        else:
            return True


@attr.s
class JobQueue(GenericQueue):

    processors = attr.ib(factory=dict)  # type: Dict[str, Processor]

    def processor(self, job_name, pass_context=False, context_var=DEFAULT_CONTEXT_VAR):
        """
        Decorator to assign processor to handle jobs with the name job_name
        from the queue queue_name

        Usage example:

        queue = sqs.queue('q1')

        @queue.processor('say_hello')
        def say_hello(name):
            print("Hello, " + name)

        Then you can add messages to the queue by calling ".delay" attribute
        of a newly created object:

        >>> say_hello.delay(name='John')

        Here, instead of calling function locally, it will be pushed to the
        queue (essentially, mimics the non-blocking call of the function).

        If you still want to call function locally, you can call

        >>> say_hello(name='John')
        """

        def fn(processor):
            return self.connect_processor(
                job_name, processor, pass_context, context_var
            )

        return fn

    def connect_processor(
        self, job_name, processor, pass_context=False, context_var=DEFAULT_CONTEXT_VAR
    ):
        """
        Assign processor (a function) to handle jobs with the name job_name
        from the queue queue_name
        """
        extra = {
            "queue_name": self.name,
            "job_name": job_name,
            "processor_name": processor.__module__ + "." + processor.__name__,
        }
        logger.debug(
            "Connect {queue_name}.{job_name} to "
            "processor {processor_name}".format(**extra),
            extra=extra,
        )
        self.processors[job_name] = self.env.processor_maker(
            queue=self,
            fn=processor,
            pass_context=pass_context,
            context_var=context_var,
            job_name=job_name,
        )
        return AsyncTask(self, job_name, processor)

    def get_processor(self, job_name):
        """
        Helper function to return a processor for the queue
        """
        return self.processors.get(job_name)

    def add_job(
        self,
        job_name,
        _content_type=codecs.DEFAULT_CONTENT_TYPE,
        _delay_seconds=None,
        _deduplication_id=None,
        _group_id=None,
        **job_kwargs
    ):
        """
        Add job to the queue. The body of the job will be converted to the text
        with one of the codes (by default it's "pickle")
        """
        codec = codecs.get_codec(_content_type)
        message_body = codec.serialize(job_kwargs)
        job_context = codec.serialize(self.env.context.to_dict())
        return self.add_raw_job(
            job_name,
            message_body,
            job_context,
            _content_type,
            _delay_seconds,
            _deduplication_id,
            _group_id,
        )

    def add_raw_job(
        self,
        job_name,
        message_body,
        job_context,
        content_type,
        delay_seconds,
        deduplication_id,
        group_id,
    ):
        """
        Low-level function to put message to the queue
        """
        # if queue name ends with .fifo, then according to the AWS specs,
        # it's a FIFO queue, and requires group_id.
        # Otherwise group_id can be set to None
        if group_id is None and self.name.endswith(".fifo"):
            group_id = DEFAULT_MESSAGE_GROUP_ID

        queue = self.get_queue()
        kwargs = {
            "MessageBody": message_body,
            "MessageAttributes": {
                "ContentType": {"StringValue": content_type, "DataType": "String"},
                "JobContext": {"StringValue": job_context, "DataType": "String"},
                "JobName": {"StringValue": job_name, "DataType": "String"},
            },
        }
        if delay_seconds is not None:
            kwargs["DelaySeconds"] = int(delay_seconds)
        if deduplication_id is not None:
            kwargs["MessageDeduplicationId"] = str(deduplication_id)
        if group_id is not None:
            kwargs["MessageGroupId"] = str(group_id)
        ret = queue.send_message(**kwargs)
        return ret["MessageId"]

    def process_message(self, message):
        # type: (Any) -> bool
        """
        Process single message.

        Return True if processing went successful
        """
        job_name = get_job_name(message)
        processor = self.get_processor(job_name)
        if processor:
            return processor.process_message(message)
        else:
            return self.process_message_fallback(message)

    def process_message_fallback(self, message):
        warnings.warn(
            "Error while processing {}.{}".format(self.name, get_job_name(message))
        )
        return False

    def copy_processors(self, dst_queue):
        # type: (JobQueue) -> None
        """
        Copy processors from self to dst_queue. Can be helpful to process d
        ead-letter queue with processors from the main queue.

        Usage example.

        sqs = SQSEnv()
        ...
        foo = sqs.queue('foo')
        foo_dead = sqs.queue('foo_dead')
        foo.copy_processors('foo_dead')
        foo_dead.process_queue(shutdown_policy=IdleShutdown(10))

        Here the queue "foo_dead" will be processed with processors from the
        queue "foo".
        """
        for job_name, processor in self.processors.items():
            dst_queue.processors[job_name] = processor.copy(queue=dst_queue)
