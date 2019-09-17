import datetime
import logging
import multiprocessing
import time
import uuid
from queue import Empty, Queue
from typing import Any, Dict

import attr

from sqs_workers import context, processors
from sqs_workers.backoff_policies import DEFAULT_BACKOFF
from sqs_workers.core import BatchProcessingResult, RedrivePolicy, get_job_name
from sqs_workers.processor_mgr import ProcessorManager
from sqs_workers.queue import GenericQueue
from sqs_workers.shutdown_policies import NeverShutdown

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

    def process_queues(self, queue_names=None, shutdown_policy_maker=NeverShutdown):
        """
        Use multiprocessing to process multiple queues at once. If queue names
        are not set, process all known queues

        shutdown_policy_maker is an optional callable which doesn't accept any
        arguments and create a new shutdown policy for each queue.

        Can looks somewhat like this:

            lambda: IdleShutdown(idle_seconds=10)
        """
        if not queue_names:
            queue_names = self.get_all_known_queues()
        processes = []
        for queue_name in queue_names:
            queue = self.queue(queue_name)
            p = multiprocessing.Process(
                target=queue.process_queue,
                kwargs={"shutdown_policy": shutdown_policy_maker()},
            )
            p.start()
            processes.append(p)
        for p in processes:
            p.join()

    def get_all_known_queues(self):
        return list(self.queues.keys())

    def get_sqs_queue_name(self, queue_name):
        return self.queue(queue_name).get_sqs_queue_name()

    def redrive_policy(self, dead_letter_queue_name, max_receive_count):
        return RedrivePolicy(self, dead_letter_queue_name, max_receive_count)


class MemoryEnvQueue(GenericQueue):
    def __init__(self, env, name):
        super(MemoryEnvQueue, self).__init__(env, name)
        self._raw_queue = Queue()
        self._queue = MemoryQueueImpl(self._raw_queue)

    def purge_queue(self):
        """
        Remove all messages from the queue
        """
        while True:
            try:
                self._raw_queue.get_nowait()
            except Empty:
                return

    def drain_queue(self, wait_seconds=0):
        """
        Delete all messages from the queue. An equivalent to purge()
        """
        self.purge_queue()

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
                self._raw_queue.put(message)
        return result

    def get_raw_messages(self, wait_seconds=0):
        """
        Helper function to get at most 10 messages from the queue, waiting for
        wait_seconds at most before they get ready.
        """
        kwargs = {
            "WaitTimeSeconds": wait_seconds,
            "MaxNumberOfMessages": 10,
            "MessageAttributeNames": ["All"],
            "AttributeNames": ["All"],
        }
        queue = self.get_queue()
        return queue.receive_messages(**kwargs)

    def _get_some_raw_messages(self, max_messages):
        """
        Helper function which returns at most max_messages from the
        queue. Used in an infinite loop inside `get_raw_messages`
        """
        return self._queue.receive_messages(MaxNumberOfMessages=max_messages)[
            "Messages"
        ]

    def get_sqs_queue_name(self):
        return self.name

    def redrive_policy(self, dead_letter_queue_name, max_receive_count):
        return RedrivePolicy(self, dead_letter_queue_name, max_receive_count)

    def get_queue(self):
        return self._queue


class MemoryQueueImpl(object):
    """
    In-memory queue which mimics the subset of SQS Queue object.

    Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
         services/sqs.html#queue
    """

    def __init__(self, _queue=None):
        if _queue is None:
            _queue = Queue()
        self._queue = _queue

    def send_message(self, **kwargs):
        message = MessageImpl.from_kwargs(kwargs)
        self._queue.put(message)
        return {"MessageId": message.message_id, "SequenceNumber": 0}

    def receive_messages(self, WaitTimeSeconds="0", MaxNumberOfMessages="10", **kwargs):
        """
        Helper function which returns at most max_messages from the
        queue. Used in an infinite loop inside `get_raw_messages`
        """
        sleep_interval = 0.1

        wait_seconds = int(WaitTimeSeconds)
        max_messages = int(MaxNumberOfMessages)
        messages = []
        while True:
            messages += self._receive_some_message(max_messages)
            if len(messages) >= max_messages:
                break
            if wait_seconds <= 0:
                break
            wait_seconds -= sleep_interval
            time.sleep(sleep_interval)
        return messages

    def _receive_some_message(self, max_number):
        messages = []
        for i in range(max_number):
            try:
                messages.append(self._queue.get_nowait())
            except Empty:
                break

        now = datetime.datetime.utcnow()
        ready_messages = []
        for message in messages:  # type: MessageImpl
            if message.execute_at > now:
                self._queue.put(message)
            else:
                ready_messages.append(message)

        return ready_messages


@attr.s(frozen=True)
class MessageImpl(object):
    """
    A mock class to mimic the AWS message

    Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
         services/sqs.html#SQS.Message
    """

    # The message's contents (not URL-encoded).
    body = attr.ib()  # type: bytes

    # Each message attribute consists of a Name, Type, and Value.
    message_attributes = attr.ib(factory=dict)  # type: Dict[str, Dict[str, str]]

    # A map of the attributes requested in `` ReceiveMessage `` to their
    # respective values.
    attributes = attr.ib(factory=dict)  # type: Dict[str, Any]

    # Internal attribute which contains the execution time.
    execute_at = attr.ib(factory=datetime.datetime.utcnow)  # type: datetime.datetime

    # A unique identifier for the message
    message_id = attr.ib(factory=lambda: uuid.uuid4().hex)  # type: str

    @classmethod
    def from_kwargs(cls, kwargs):
        """
        Make a message from kwargs, as provided by queue.send_message():

        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
             services/sqs.html#SQS.Queue.send_message
        """
        # required attributes
        body = kwargs["MessageBody"]
        message_atttributes = kwargs["MessageAttributes"]

        # optional attributes
        attributes = {}
        if "MessageDeduplicationId" in kwargs:
            attributes["MessageDeduplicationId"] = kwargs["MessageDeduplicationId"]

        if "MessageGroupId" in kwargs:
            attributes["MessageGroupId"] = kwargs["MessageGroupId"]

        execute_at = datetime.datetime.utcnow()
        if "DelaySeconds" in kwargs:
            delay_seconds_int = int(kwargs["DelaySeconds"])
            execute_at += datetime.timedelta(seconds=delay_seconds_int)

        return MessageImpl(body, message_atttributes, attributes, execute_at)
