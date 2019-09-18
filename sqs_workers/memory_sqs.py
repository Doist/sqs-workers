"""
In memory mockup implementation of essential parts of boto3 SQS objects.

Used for faster and more predictable processing in tests.

Lacks some features of "real sqs", and some other features implemented
ineffectively.

- Redrive policy doesn't work
- There is no differences between standard and FIFO queues
- FIFO queues don't support content-based deduplication
- Delayed tasks executed ineffectively: the task is gotten from the queue,
  and if the time hasn't come yet, the task is put back.
- API can return slightly different results

Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
"""
import datetime
import time
import uuid
from queue import Empty, Queue
from typing import Any, Dict, List

import attr


@attr.s()
class MemoryAWS(object):
    """
    In-memory AWS as a whole.
    """

    client = attr.ib(
        repr=False,
        default=attr.Factory(lambda self: MemoryClient(self), takes_self=True),
    )  # type: "Client"
    resource = attr.ib(
        repr=False,
        default=attr.Factory(lambda self: ServiceResource(self), takes_self=True),
    )  # type: "ServiceResource"
    queues = attr.ib(factory=list)  # type: List["MemoryQueueImpl"]

    def create_queue(self, QueueName, Attributes):
        queue = MemoryQueue(self, QueueName, Attributes)
        self.queues.append(queue)
        return queue

    def delete_queue(self, QueueUrl):
        self.queues = [queue for queue in self.queues if queue.url != QueueUrl]


@attr.s()
class MemorySession(object):
    """
    In memory AWS session.
    """

    aws = attr.ib(repr=False, factory=MemoryAWS)

    def client(self, service_name):
        assert service_name == "sqs"
        return self.aws.client

    def resource(self, service_name):
        assert service_name == "sqs"
        return self.aws.resource


@attr.s()
class MemoryClient(object):

    aws = attr.ib(repr=False)

    def create_queue(self, QueueName, Attributes):
        return self.aws.create_queue(QueueName, Attributes)

    def delete_queue(self, QueueUrl):
        return self.aws.delete_queue(QueueUrl)

    def list_queues(self, QueueNamePrefix=""):
        return {
            "QueueUrls": [
                queue.url
                for queue in self.aws.queues
                if queue.name.startswith(QueueNamePrefix)
            ]
        }


@attr.s()
class ServiceResource(object):

    aws = attr.ib(repr=False)  # type: MemoryAWS

    def create_queue(self, QueueName, Attributes):
        return self.aws.create_queue(QueueName, Attributes)

    def get_queue_by_name(self, QueueName):
        for queue in self.aws.queues:
            if queue.name == QueueName:
                return queue
        return None


@attr.s()
class MemoryQueue(object):
    """
    In-memory queue which mimics the subset of SQS Queue object.

    Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
         services/sqs.html#queue
    """

    aws = attr.ib()  # type: MemoryAWS
    name = attr.ib()  # type: str
    attributes = attr.ib()  # type: Dict[str, Dict[str, str]]
    _queue = attr.ib(factory=Queue)  # type: Queue

    def __attrs_post_init__(self):
        self.attributes["QueueArn"] = self.name

    @property
    def url(self):
        return "memory://{}".format(self.name)

    def send_message(self, **kwargs):
        message = MemoryMessage.from_kwargs(self, kwargs)
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
        for message in messages:  # type: MemoryMessage
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
                message = self._queue.get_nowait()  # type: MemoryMessage
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

    def delete(self):
        return self.aws.delete_queue(self.url)

    def __dict__(self):
        return {"QueueUrl": self.url}

    def __getitem__(self, item):
        return self.__dict__()[item]


@attr.s(frozen=True)
class MemoryMessage(object):
    """
    A mock class to mimic the AWS message

    Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
         services/sqs.html#SQS.Message
    """

    queue_impl = attr.ib()  # type: MemoryQueue

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

        return MemoryMessage(
            queue_impl, body, message_atttributes, attributes, execute_at
        )

    def change_visibility(self, VisibilityTimeout="0", **kwargs):
        timeout = int(VisibilityTimeout)
        execute_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout)
        message = attr.evolve(self, execute_at=execute_at)
        message.attributes["ApproximateReceiveCount"] += 1
        self.queue_impl._queue.put_nowait(message)
