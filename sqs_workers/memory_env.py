import datetime
import logging
import multiprocessing
import time
import uuid
from queue import Empty, Queue

from sqs_workers import codecs, context, processors
from sqs_workers.backoff_policies import DEFAULT_BACKOFF
from sqs_workers.codecs import DEFAULT_CONTENT_TYPE
from sqs_workers.core import BatchProcessingResult, RedrivePolicy, get_job_name
from sqs_workers.processor_mgr import ProcessorManager, ProcessorManagerProxy
from sqs_workers.shutdown_policies import NEVER_SHUTDOWN, NeverShutdown

logger = logging.getLogger(__name__)


class MemoryEnv(object):
    """
    In-memory pseudo SQS implementation, for faster and more predictable
    processing in tests.

    Implements the SQSEnv interface, but lacks some features of it, and some
    other features implemented ineffectively.

    - Redrive policy doesn't work
    - There is no differences between standard and FIFO queues
    - FIFO queues don't support content-based deduplication
    - Delayed tasks executed ineffectively: the task is gotten from the queue,
      and if the time hasn't come yet, the task is put back.
    - API can return slightly different results
    """

    # TODO: maybe it can be implemented more effectively with sched

    def __init__(
        self,
        backoff_policy=DEFAULT_BACKOFF,
        processor_maker=processors.Processor,
        fallback_processor_maker=processors.FallbackProcessor,
        context_maker=context.SQSContext,
    ):
        """
        Initialize pseudo SQS environment
        """
        self.backoff_policy = backoff_policy
        self.context = context_maker()
        self.processors = ProcessorManager(
            self, backoff_policy, processor_maker, fallback_processor_maker
        )
        self.queues = {}  # type: dict[str, MemoryEnvQueue]

    def queue(self, queue_name):
        if queue_name not in self.queues:
            self.queues[queue_name] = MemoryEnvQueue(self, queue_name)
        return self.queues[queue_name]

    def purge_queue(self, queue_name):
        """
        Remove all messages from the queue
        """
        return self.queue(queue_name).purge_queue()

    def add_job(
        self,
        queue_name,
        job_name,
        _content_type=DEFAULT_CONTENT_TYPE,
        _delay_seconds=None,
        _deduplication_id=None,
        _group_id=None,
        **job_kwargs
    ):
        """
        Add job to the queue. The body of the job will be converted to the text
        with one of the codes (by default it's "pickle")
        """
        return self.queue(queue_name).add_job(
            job_name,
            _content_type,
            _delay_seconds,
            _deduplication_id,
            _group_id,
            **job_kwargs
        )

    def add_raw_job(
        self,
        queue_name,
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
        return self.queue(queue_name).add_raw_job(
            job_name,
            message_body,
            job_context,
            content_type,
            delay_seconds,
            deduplication_id,
            group_id,
        )

    def process_queues(self, queue_names=None, shutdown_policy_maker=NeverShutdown):
        """
        Use multiprocessing to process multiple queues at once. If queue names
        are not set, process all known queues

        shutdown_policy_maker is an optional callable which doesn't accept any
        arguments and create a new shutdown policy for each queue.
        """
        if not queue_names:
            queue_names = self.get_all_known_queues()
        processes = []
        for queue_name in queue_names:
            p = multiprocessing.Process(
                target=self.process_queue,
                kwargs={
                    "queue_name": queue_name,
                    "shutdown_policy": shutdown_policy_maker(),
                },
            )
            p.start()
            processes.append(p)
        for p in processes:
            p.join()

    def drain_queue(self, queue_name, wait_seconds=0):
        """
        Delete all messages from the queue. An equivalent to purge()
        """
        return self.queue(queue_name).drain_queue(wait_seconds)

    def process_queue(self, queue_name, shutdown_policy=NEVER_SHUTDOWN, wait_second=10):
        """
        Run worker to process one queue in the infinite loop
        """
        return self.queue(queue_name).process_queue(shutdown_policy, wait_second)

    def process_batch(self, queue_name, wait_seconds=0):
        # type: (str, int) -> BatchProcessingResult
        """
        Process a batch of messages from the queue (10 messages at most), return
        the number of successfully processed messages, and exit
        """
        return self.queue(queue_name).process_batch(wait_seconds)

    def get_raw_messages(self, queue_name, wait_seconds=0):
        """
        Helper function to get at most 10 messages from the queue, waiting for
        wait_seconds at most before they get ready.
        """
        return self.queue(queue_name).get_raw_messages(wait_seconds)

    def get_all_known_queues(self):
        return list(self.queues.keys())

    def get_sqs_queue_name(self, queue_name):
        return self.queue(queue_name).get_sqs_queue_name()

    def redrive_policy(self, dead_letter_queue_name, max_receive_count):
        return RedrivePolicy(self, dead_letter_queue_name, max_receive_count)


class MemoryEnvQueue(ProcessorManagerProxy):
    def __init__(self, env, name):
        # type: (MemoryEnv, str) -> None
        self.env = env
        self.name = name
        self._queue = Queue()

    def purge_queue(self):
        """
        Remove all messages from the queue
        """
        while True:
            try:
                self._queue.get_nowait()
            except Empty:
                return

    def add_job(
        self,
        job_name,
        _content_type=DEFAULT_CONTENT_TYPE,
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
        kwargs = {
            "MessageBody": message_body,
            "MessageAttributes": {
                "ContentType": {"StringValue": content_type, "DataType": "String"},
                "JobContext": {"StringValue": job_context, "DataType": "String"},
                "JobName": {"StringValue": job_name, "DataType": "String"},
            },
        }
        if delay_seconds is not None:
            delay_seconds_int = int(delay_seconds)
            execute_at = datetime.datetime.utcnow() + datetime.timedelta(
                seconds=delay_seconds
            )
            kwargs["DelaySeconds"] = delay_seconds_int
            kwargs["_execute_at"] = execute_at
        self._queue.put(kwargs)
        return ""

    def drain_queue(self, wait_seconds=0):
        """
        Delete all messages from the queue. An equivalent to purge()
        """
        self.purge_queue()

    def process_queue(self, shutdown_policy=NEVER_SHUTDOWN, wait_second=10):
        """
        Run worker to process one queue in the infinite loop
        """
        while True:
            result = self.process_batch(wait_seconds=wait_second)
            shutdown_policy.update_state(result)
            if shutdown_policy.need_shutdown():
                break

    def process_batch(self, wait_seconds=0):
        # type: (int) -> BatchProcessingResult
        """
        Process a batch of messages from the queue (10 messages at most), return
        the number of successfully processed messages, and exit
        """
        messages = self.get_raw_messages(wait_seconds)
        result = BatchProcessingResult(self.name)
        for message in messages:
            job_name = get_job_name(message)
            processor = self.env.processors.get(self.name, job_name)
            success = processor.process_message(message)
            result.update_with_message(message, success)
            if not success:
                self._queue.put(message)
        return result

    def get_raw_messages(self, wait_seconds=0):
        """
        Helper function to get at most 10 messages from the queue, waiting for
        wait_seconds at most before they get ready.
        """
        max_messages = 10
        sleep_interval = 0.1

        messages = []
        while True:
            messages += self._get_some_raw_messages(max_messages)
            if len(messages) >= max_messages:
                break
            if wait_seconds <= 0:
                break
            wait_seconds -= sleep_interval
            time.sleep(sleep_interval)

        return messages

    def _get_some_raw_messages(self, max_messages):
        """
        Helper function which returns at most max_messages from the
        queue. Used in an infinite loop inside `get_raw_messages`
        """
        messages = []
        for i in range(max_messages):
            try:
                messages.append(RawMessage(self._queue.get_nowait()))
            except Empty:
                break

        now = datetime.datetime.utcnow()
        ready_messages = []
        for message in messages:
            execute_at = message.get("_execute_at", now)
            if execute_at > now:
                self._queue.put(message)
            else:
                ready_messages.append(message)

        return ready_messages

    def get_sqs_queue_name(self):
        return self.name

    def redrive_policy(self, dead_letter_queue_name, max_receive_count):
        return RedrivePolicy(self, dead_letter_queue_name, max_receive_count)


class RawMessage(dict):
    """
    A mock class to mimic the AWS message
    """

    @property
    def message_attributes(self):
        return self["MessageAttributes"]

    @property
    def body(self):
        return self["MessageBody"]

    @property
    def message_id(self):
        return uuid.uuid4().hex
