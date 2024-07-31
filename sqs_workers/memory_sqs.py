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
import logging
import uuid
from typing import Any, Dict, List

import attr

logger = logging.getLogger(__name__)


@attr.s
class MemoryAWS:
    """In-memory AWS as a whole."""

    client: "MemoryClient" = attr.ib(
        repr=False,
        default=attr.Factory(lambda self: MemoryClient(self), takes_self=True),
    )
    resource: "ServiceResource" = attr.ib(
        repr=False,
        default=attr.Factory(lambda self: ServiceResource(self), takes_self=True),
    )
    queues: List["MemoryQueue"] = attr.ib(factory=list)

    def create_queue(self, QueueName: str, Attributes) -> "MemoryQueue":
        queue = MemoryQueue(self, QueueName, Attributes)
        self.queues.append(queue)
        return queue

    def delete_queue(self, QueueUrl: str) -> None:
        self.queues = [queue for queue in self.queues if queue.url != QueueUrl]


@attr.s
class MemorySession:
    """In memory AWS session."""

    aws = attr.ib(repr=False, factory=MemoryAWS)

    def client(self, service_name: str, **kwargs):
        assert service_name == "sqs"
        return self.aws.client

    def resource(self, service_name: str, **kwargs):
        assert service_name == "sqs"
        return self.aws.resource


@attr.s
class MemoryClient:
    aws = attr.ib(repr=False)

    def create_queue(self, QueueName: str, Attributes):
        return self.aws.create_queue(QueueName, Attributes)

    def delete_queue(self, QueueUrl: str):
        return self.aws.delete_queue(QueueUrl)

    def list_queues(self, QueueNamePrefix=""):
        return {
            "QueueUrls": [
                queue.url
                for queue in self.aws.queues
                if queue.name.startswith(QueueNamePrefix)
            ]
        }


@attr.s
class ServiceResource:
    aws: MemoryAWS = attr.ib(repr=False)

    def create_queue(self, QueueName: str, Attributes):
        return self.aws.create_queue(QueueName, Attributes)

    def get_queue_by_name(self, QueueName: str):
        for queue in self.aws.queues:
            if queue.name == QueueName:
                return queue
        return None


@attr.s
class MemoryQueue:
    """
    In-memory queue which mimics the subset of SQS Queue object.

    Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
         services/sqs.html#queue
    """

    aws: MemoryAWS = attr.ib(repr=False)
    name: str = attr.ib()
    attributes: Dict[str, Dict[str, str]] = attr.ib()
    messages: List["MemoryMessage"] = attr.ib(factory=list)
    in_flight: Dict[str, "MemoryMessage"] = attr.ib(factory=dict)

    def __attrs_post_init__(self):
        self.attributes["QueueArn"] = self.name

    @property
    def url(self):
        return f"memory://{self.name}"

    def send_message(self, **kwargs):
        message = MemoryMessage.from_kwargs(self, kwargs)
        self.messages.append(message)
        return {"MessageId": message.message_id, "SequenceNumber": 0}

    def send_messages(self, Entries):
        res = []
        for message in Entries:
            res.append(self.send_message(**message))
        return {"Successful": res, "Failed": []}

    def receive_messages(self, WaitTimeSeconds="0", MaxNumberOfMessages="10", **kwargs):
        """
        Helper function which returns at most max_messages from the
        queue. Used in an infinite loop inside `get_raw_messages`
        """
        wait_seconds = int(WaitTimeSeconds)
        max_messages = int(MaxNumberOfMessages)

        ready_messages = []
        push_back_messages = []

        now = datetime.datetime.utcnow()
        threshold = now + datetime.timedelta(seconds=wait_seconds)

        # before retrieving messages, go through in_flight and return any
        # messages whose "invisible" timeout has expired back to the pool
        for message_id, message in self.in_flight.copy().items():
            if message.visible_at <= threshold:
                self.in_flight.pop(message_id)
                self.messages.append(message)

        for message in self.messages:
            if message.visible_at > threshold or len(ready_messages) >= max_messages:
                push_back_messages.append(message)
            else:
                ready_messages.append(message)

        self.messages[:] = push_back_messages

        # now, mark all returned messages as "in flight" and unavailable
        # to be returned until their invisibility timeout comes (1s)
        for m in ready_messages:
            self.in_flight[m.message_id] = m
            m.change_visibility(VisibilityTimeout="1")

        return ready_messages

    def delete_messages(self, Entries):
        """
        Delete messages implementation.

        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
             services/sqs.html#SQS.Queue.delete_messages
        """
        found_entries = []
        not_found_entries = []

        for e in Entries:
            if e["Id"] in self.in_flight:
                found_entries.append(e)
                self.in_flight.pop(e["Id"])
            else:
                not_found_entries.append(e)

        return {
            "Successful": [{"Id": e["Id"]} for e in found_entries],
            "Failed": [{"Id": e["Id"]} for e in not_found_entries],
        }

    def change_message_visibility_batch(self, Entries):
        """
        Changes message visibility by looking at in-flight messages, setting
        a new visible_at, when it will return to the pool of messages
        """
        found_entries = []
        not_found_entries = []

        now = datetime.datetime.utcnow()

        for e in Entries:
            if e["Id"] in self.in_flight:
                found_entries.append(e)
                in_flight_message = self.in_flight[e["Id"]]
                sec = int(e["VisibilityTimeout"])
                visible_at = now + datetime.timedelta(seconds=sec)
                updated_message = attr.evolve(in_flight_message, visible_at=visible_at)
                self.in_flight[e["Id"]] = updated_message
            else:
                not_found_entries.append(e)

        return {
            "Successful": [{"Id": e["Id"]} for e in found_entries],
            "Failed": [{"Id": e["Id"]} for e in not_found_entries],
        }

    def delete(self):
        return self.aws.delete_queue(self.url)

    def __dict__(self):
        return {"QueueUrl": self.url}

    def __getitem__(self, item):
        return self.__dict__()[item]


@attr.s(frozen=True)
class MemoryMessage:
    """
    A mock class to mimic the AWS message

    Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
         services/sqs.html#SQS.Message
    """

    queue_impl: MemoryQueue = attr.ib(repr=False)

    # The message's contents (not URL-encoded).
    body: bytes = attr.ib()

    # Each message attribute consists of a Name, Type, and Value.
    message_attributes: Dict[str, Dict[str, str]] = attr.ib(factory=dict)

    # A map of the attributes requested in `` ReceiveMessage `` to their
    # respective values.
    attributes: Dict[str, Any] = attr.ib(factory=dict)

    # Internal attribute which contains the execution time.
    visible_at: datetime.datetime = attr.ib(factory=datetime.datetime.utcnow)

    # A unique identifier for the message
    message_id: str = attr.ib(factory=lambda: uuid.uuid4().hex)

    # The Message's receipt_handle identifier
    receipt_handle: str = attr.ib(factory=lambda: uuid.uuid4().hex)

    @classmethod
    def from_kwargs(cls, queue_impl, kwargs):
        """
        Make a message from kwargs, as provided by queue.send_message():

        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
             services/sqs.html#SQS.Queue.send_message
        """
        # required attributes
        body = kwargs["MessageBody"]
        message_atttributes = kwargs.get("MessageAttributes")

        # optional attributes
        attributes = {"ApproximateReceiveCount": 1}
        if "MessageDeduplicationId" in kwargs:
            attributes["MessageDeduplicationId"] = kwargs["MessageDeduplicationId"]

        if "MessageGroupId" in kwargs:
            attributes["MessageGroupId"] = kwargs["MessageGroupId"]

        visible_at = datetime.datetime.utcnow()
        if "DelaySeconds" in kwargs:
            delay_seconds_int = int(kwargs["DelaySeconds"])
            visible_at += datetime.timedelta(seconds=delay_seconds_int)

        return MemoryMessage(
            queue_impl, body, message_atttributes, attributes, visible_at
        )

    def change_visibility(self, VisibilityTimeout="0", **kwargs):
        if self.message_id in self.queue_impl.in_flight:
            now = datetime.datetime.utcnow()
            sec = int(VisibilityTimeout)
            visible_at = now + datetime.timedelta(seconds=sec)
            updated_message = attr.evolve(self, visible_at=visible_at)
            self.queue_impl.in_flight[self.message_id] = updated_message
        else:
            logger.warning("Tried to change visibility of message not in flight")
