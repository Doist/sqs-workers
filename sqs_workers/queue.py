import logging
import uuid
from collections import defaultdict
from contextlib import contextmanager
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

import attr

from sqs_workers import DEFAULT_BACKOFF, codecs
from sqs_workers.async_task import AsyncTask
from sqs_workers.backoff_policies import BackoffPolicy
from sqs_workers.batching import BatchingConfiguration, NoBatching
from sqs_workers.core import BatchProcessingResult, get_job_name
from sqs_workers.exceptions import SQSError
from sqs_workers.processors import DEFAULT_CONTEXT_VAR, Processor
from sqs_workers.shutdown_policies import NEVER_SHUTDOWN

DEFAULT_MESSAGE_GROUP_ID = "default"
SEND_BATCH_SIZE = 10

if TYPE_CHECKING:
    from sqs_workers import SQSEnv

logger = logging.getLogger(__name__)


class SQSBatchError(SQSError):
    def __init__(self, errors):
        self.errors = errors


@attr.s
class GenericQueue(object):
    env: "SQSEnv" = attr.ib(repr=False)
    name: str = attr.ib()
    backoff_policy: BackoffPolicy = attr.ib(default=DEFAULT_BACKOFF)
    batching_policy: BatchingConfiguration = attr.ib(default=NoBatching())
    _queue = attr.ib(repr=False, default=None)

    @classmethod
    def maker(cls, **kwargs):
        return partial(cls, **kwargs)

    def process_queue(self, shutdown_policy=NEVER_SHUTDOWN, wait_second=10):
        """
        Run worker to process one queue in the infinite loop
        """
        logger.debug(
            "Start processing queue %s",
            self.name,
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
                    "Stop processing queue %s",
                    self.name,
                    extra={
                        "queue_name": self.name,
                        "wait_seconds": wait_second,
                        "shutdown_policy": repr(shutdown_policy),
                    },
                )
                break

    def process_batch(self, wait_seconds=0) -> BatchProcessingResult:
        """
        Process a batch of messages from the queue (10 messages at most), return
        the number of successfully processed messages, and exit
        """
        queue = self.get_queue()

        if self.batching_policy.batching_enabled:
            return self._process_messages_in_batch(queue, wait_seconds)

        return self._process_messages_individually(queue, wait_seconds)

    def _process_messages_in_batch(self, queue, wait_seconds):
        messages = self.get_raw_messages(wait_seconds, self.batching_policy.batch_size)
        result = BatchProcessingResult(self.name)

        success = self.process_messages(messages)

        for message in messages:
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

    def _process_messages_individually(self, queue, wait_seconds):
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

    def process_message(self, message: Any) -> bool:
        """
        Process single message.

        Return True if processing went successful
        """
        raise NotImplementedError()

    def process_messages(self, messages: List[Any]) -> bool:
        """
        Process a batch of messages.

        Return True if processing went successful
        for every message in the batch
        """
        raise NotImplementedError()

    def get_raw_messages(self, wait_seconds, max_messages=10):
        """
        Return raw messages from the queue, addressed by its name
        """
        kwargs = {
            "WaitTimeSeconds": wait_seconds,
            "MaxNumberOfMessages": max_messages if max_messages <= 10 else 10,
            "MessageAttributeNames": ["All"],
            "AttributeNames": ["All"],
        }
        queue = self.get_queue()

        if max_messages <= 10:
            return queue.receive_messages(**kwargs)

        messages_left_on_queue = True
        received_messages = []

        # receive_messages will only return a maximum batch of 10 messages per call
        # if the client requests more than that we must poll multiple times
        while messages_left_on_queue and len(received_messages) < max_messages:
            messages = queue.receive_messages(**kwargs)
            received_messages.extend(messages)
            if len(messages) < 10:
                messages_left_on_queue = False

        return received_messages

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
    processor: Optional[Callable] = attr.ib(default=None)

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
            "Connect %s to processor %s",
            extra["queue_name"],
            extra["processor_name"],
            extra=extra,
        )
        self.processor = processor

    def add_raw_job(
        self,
        message_body: str,
        delay_seconds: int = 0,
        deduplication_id: Optional[str] = None,
        group_id: str = DEFAULT_MESSAGE_GROUP_ID,
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

    def process_message(self, message: Any) -> bool:
        """
        Sends a single message to the call handler.

        Return True if processing went successful
        """
        extra = {"message_id": message.message_id, "queue_name": self.name}
        if not self.processor:
            logger.warning("No processor set for %s", extra["queue_name"], extra=extra)
            return False

        logger.debug(
            "Process %s.%s", extra["queue_name"], extra["message_id"], extra=extra
        )

        try:
            self.processor(message)
        except Exception:
            logger.exception(
                "Error while processing %s.%s",
                extra["queue_name"],
                extra["message_id"],
                extra=extra,
            )
            return False
        else:
            return True

    def process_messages(self, messages: List[Any]) -> bool:
        """
        Sends a list of messages to the call handler

        Return True if processing went successful for all messages in the batch.
        """
        message_ids = [m.message_id for m in messages]
        extra = {"message_ids": message_ids, "queue_name": self.name}
        if not self.processor:
            logger.warning(
                "No processor set for %s",
                extra["queue_name"],
                extra=extra,
            )
            return False

        logger.debug(
            "Process %s for ids %s",
            extra["queue_name"],
            extra["message_ids"],
            extra=extra,
        )

        try:
            self.processor(messages)
        except Exception:
            logger.exception(
                "Error while processing %s for ids %s",
                extra["queue_name"],
                extra["message_ids"],
                extra=extra,
            )
            return False
        else:
            return True


@attr.s
class JobQueue(GenericQueue):
    processors: Dict[str, Processor] = attr.ib(factory=dict)

    _batch_level: int = attr.ib(default=0, repr=False)
    _batched_messages: List[Dict] = attr.ib(factory=list, repr=False)

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

            say_hello.delay(name='John')

        Here, instead of calling function locally, it will be pushed to the
        queue (essentially, mimics the non-blocking call of the function).

        If you still want to call function locally, you can call

            say_hello(name='John')
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
            "Connect %s.%s to processor %s",
            extra["queue_name"],
            extra["job_name"],
            extra["processor_name"],
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

    def open_add_batch(self):
        """
        Open an add batch.
        """
        self._batch_level += 1

    def close_add_batch(self):
        """
        Close an add batch.
        """
        self._batch_level = max(0, self._batch_level - 1)
        self._flush_batch_if_needed()

    @contextmanager
    def add_batch(self):
        """
        Context manager to add jobs in batch.

        Inside this context manager, jobs won't be added to the queue
        immediately, but grouped by batches of 10. Once open, new jobs won't be
        sent to SQS immediately, but added to a local cache. Then, once this
        cache is big enough or when closing the batch, all messages will be
        sent, using as few requests as possible.
        """
        self.open_add_batch()
        try:
            yield
        finally:
            self.close_add_batch()

    def _should_flush_batch(self):
        """
        Check if a message batch should be flushed, i.e. all messages should be sent
        and removed from the internal cache.

        Right now this only checks the number of messages. In the future we may
        want to improve that by also ensuring the batch size is small enough.
        """
        max_size = SEND_BATCH_SIZE if self._batch_level > 0 else 1
        return len(self._batched_messages) >= max_size

    def _flush_batch_if_needed(self):
        queue = self.get_queue()

        # There should be at most 1 batch to send. But just in case, prepare to
        # send more than that.
        while self._should_flush_batch():
            msgs = self._batched_messages[:SEND_BATCH_SIZE]
            self._batched_messages = self._batched_messages[SEND_BATCH_SIZE:]

            # Add Ids
            msg_by_id = {}
            for msg in msgs:
                id_ = uuid.uuid4().hex
                msg["Id"] = id_
                msg_by_id[id_] = msg

            res = queue.send_messages(Entries=msgs)

            # Handle errors
            if res.get("Failed", []):
                errors = res["Failed"]
                for error in errors:
                    error["_message"] = msg_by_id[error["Id"]]
                raise SQSBatchError(errors)

    def add_job(
        self,
        job_name,
        _content_type=None,
        _delay_seconds=None,
        _deduplication_id=None,
        _group_id=None,
        **job_kwargs
    ):
        """
        Add job to the queue. The body of the job will be converted to the text
        with one of the codecs (by default it's "pickle_compat")
        """
        if not _content_type:
            _content_type = self.env.codec
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

        if self._batch_level > 0:
            self._batched_messages.append(kwargs)
            self._flush_batch_if_needed()
            return None
        else:
            ret = queue.send_message(**kwargs)
            return ret["MessageId"]

    def process_message(self, message: Any) -> bool:
        """
        Sends a single message to the call handler.

        Return True if processing went successful
        """
        job_name = get_job_name(message)
        processor = self.get_processor(job_name)
        if processor:
            return processor.process_message(message)
        else:
            return self.process_message_fallback(job_name)

    def process_messages(self, messages: List[Any]) -> bool:
        """
        Sends a list of messages to the call handler

        Return True if processing went successful for all messages in the batch.
        """
        messages_by_job_name = defaultdict(list)
        for message in messages:
            job_name = get_job_name(message)
            messages_by_job_name[job_name].append(message)

        results = []

        for job_name, grouped_messages in messages_by_job_name.items():
            processor = self.get_processor(job_name)
            if processor:
                result = processor.process_messages(grouped_messages)
                results.append(result)
            else:
                result = self.process_message_fallback(job_name)
                results.append(result)

        return all(results)

    def process_message_fallback(self, job_name):
        # Note: we don't set exc_info=True, since source of the error is almost
        # certainly in another code base.
        logger.error(
            "Could not find an SQS processor for %s.%s. "
            "Has it been registered correctly?",
            self.name,
            job_name,
        )
        return False

    def copy_processors(self, dst_queue: "JobQueue"):
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
