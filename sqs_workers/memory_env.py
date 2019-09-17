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
from sqs_workers.core import RedrivePolicy
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
        self._queue = MemoryQueueImpl()

    def get_queue(self):
        return self._queue


class MemoryQueueImpl(object):
    """
    In-memory queue which mimics the subset of SQS Queue object.

    Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
         services/sqs.html#queue
    """

    def __init__(self):
        self._queue = Queue()

    def send_message(self, **kwargs):
        message = MessageImpl.from_kwargs(self, kwargs)
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

    def delete_messages(self, Entries):
        """
        Delete messages implementation.

        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
             services/sqs.html#SQS.Queue.delete_messages
        """
        message_ids = {entry["Id"] for entry in Entries}

        successfully_deleted = set()
        messages_to_push_back = []
        while True:
            try:
                message = self._queue.get_nowait()  # type: MessageImpl
                if message.message_id in message_ids:
                    successfully_deleted.add(message.message_id)
                else:
                    messages_to_push_back.append(message)
            except Empty:
                break

        for message in messages_to_push_back:
            self._queue.put_nowait(message)

        didnt_deleted = message_ids.difference(successfully_deleted)
        return {
            "Successful": [{"Id": _id} for _id in successfully_deleted],
            "Failed": [{"Id": _id} for _id in didnt_deleted],
        }


@attr.s(frozen=True)
class MessageImpl(object):
    """
    A mock class to mimic the AWS message

    Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
         services/sqs.html#SQS.Message
    """

    queue_impl = attr.ib()  # type: MemoryQueueImpl

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

    # The Message's receipt_handle identifier
    receipt_handle = attr.ib(factory=lambda: uuid.uuid4().hex)  # type: str

    @classmethod
    def from_kwargs(cls, queue_impl, kwargs):
        """
        Make a message from kwargs, as provided by queue.send_message():

        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
             services/sqs.html#SQS.Queue.send_message
        """
        # required attributes
        body = kwargs["MessageBody"]
        message_atttributes = kwargs["MessageAttributes"]

        # optional attributes
        attributes = {"ApproximateReceiveCount": 1}
        if "MessageDeduplicationId" in kwargs:
            attributes["MessageDeduplicationId"] = kwargs["MessageDeduplicationId"]

        if "MessageGroupId" in kwargs:
            attributes["MessageGroupId"] = kwargs["MessageGroupId"]

        execute_at = datetime.datetime.utcnow()
        if "DelaySeconds" in kwargs:
            delay_seconds_int = int(kwargs["DelaySeconds"])
            execute_at += datetime.timedelta(seconds=delay_seconds_int)

        return MessageImpl(
            queue_impl, body, message_atttributes, attributes, execute_at
        )

    def change_visibility(self, VisibilityTimeout="0", **kwargs):
        timeout = int(VisibilityTimeout)
        execute_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout)
        message = attr.evolve(self, execute_at=execute_at)
        message.attributes["ApproximateReceiveCount"] += 1
        self.queue_impl._queue.put_nowait(message)
