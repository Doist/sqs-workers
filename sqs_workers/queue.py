import logging
from typing import TYPE_CHECKING, Any, Dict

import attr

from sqs_workers import codecs
from sqs_workers.async_task import AsyncTask
from sqs_workers.backoff_policies import BackoffPolicy
from sqs_workers.core import BatchProcessingResult, get_job_name
from sqs_workers.processors import DEFAULT_CONTEXT_VAR, GenericProcessor
from sqs_workers.shutdown_policies import NEVER_SHUTDOWN

DEFAULT_MESSAGE_GROUP_ID = "default"

if TYPE_CHECKING:
    from sqs_workers import SQSEnv


logger = logging.getLogger(__name__)


@attr.s
class GenericQueue(object):
    env = attr.ib()  # type: SQSEnv
    name = attr.ib()  # type: str
    _queue = attr.ib(default=None)

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
                timeout = self.get_backoff_policy(message).get_visibility_timeout(
                    message
                )
                message.change_visibility(VisibilityTimeout=timeout)
        return result

    def process_message(self, message):
        # type: (Any) -> bool
        """
        Process single message.

        Return True if processing went successful
        """
        raise NotImplementedError()

    def get_backoff_policy(self, message):
        # type: (Any) -> BackoffPolicy
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
        return "{}{}".format(self.env.queue_prefix, self.name)

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
    processor = attr.ib(default=None)  # type: GenericProcessor


@attr.s
class SQSQueue(GenericQueue):

    processors = attr.ib(factory=dict)  # type: Dict[str, GenericProcessor]

    def processor(
        self,
        job_name,
        pass_context=False,
        context_var=DEFAULT_CONTEXT_VAR,
        backoff_policy=None,
    ):
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
                job_name, processor, pass_context, context_var, backoff_policy
            )

        return fn

    def connect_processor(
        self,
        job_name,
        processor,
        pass_context=False,
        context_var=DEFAULT_CONTEXT_VAR,
        backoff_policy=None,
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
            backoff_policy=backoff_policy or self.env.backoff_policy,
            job_name=job_name,
        )
        return AsyncTask(self, job_name, processor)

    def get_processor(self, job_name):
        """
        Helper function to return a processor for the queue
        """
        processor = self.processors.get(job_name)
        if processor is None:
            processor = self.env.fallback_processor_maker(queue=self, job_name=job_name)
            self.processors[job_name] = processor
        return processor

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
        return processor.process_message(message)

    def get_backoff_policy(self, message):
        # type: (Any) -> BackoffPolicy
        job_name = get_job_name(message)
        processor = self.get_processor(job_name)
        return processor.backoff_policy

    def copy_processors(self, dst_queue):
        # type: (SQSQueue) -> None
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
