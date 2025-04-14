import logging
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class MemoryAWS:
    """In-memory AWS as a whole."""

    client: Optional["MemoryClient"] = field(repr=False, default=None)
    resource: Optional["ServiceResource"] = field(repr=False, default=None)
    queues: list["MemoryQueue"] = field(default_factory=list)

    def __post_init__(self):
        if self.client is None:
            self.client = MemoryClient(self)
        if self.resource is None:
            self.resource = ServiceResource(self)

    def create_queue(self, QueueName: str, Attributes) -> "MemoryQueue":
        queue = MemoryQueue(self, QueueName, Attributes)
        self.queues.append(queue)
        return queue

    def delete_queue(self, QueueUrl: str) -> None:
        self.queues = [queue for queue in self.queues if queue.url != QueueUrl]


@dataclass
class MemorySession:
    """In memory AWS session."""

    aws: MemoryAWS = field(repr=False, default_factory=MemoryAWS)

    def client(self, service_name: str, **kwargs):
        assert service_name == "sqs"
        return self.aws.client

    def resource(self, service_name: str, **kwargs):
        assert service_name == "sqs"
        return self.aws.resource


@dataclass
class MemoryClient:
    aws: Any = field(repr=False)

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


@dataclass
class ServiceResource:
    aws: MemoryAWS = field(repr=False)

    def create_queue(self, QueueName: str, Attributes):
        return self.aws.create_queue(QueueName, Attributes)

    def get_queue_by_name(self, QueueName: str):
        for queue in self.aws.queues:
            if queue.name == QueueName:
                return queue
        return None


@dataclass
class MemoryQueue:
    """
    In-memory queue which mimics the subset of SQS Queue object.

    Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
         services/sqs.html#queue
    """

    aws: MemoryAWS = field(repr=False)
    name: str = field()
    attributes: dict[str, dict[str, str]] = field()
    messages: list["MemoryMessage"] = field(default_factory=list)
    in_flight: dict[str, "MemoryMessage"] = field(default_factory=dict)

    def __post_init__(self):
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

        now = datetime.now(tz=timezone.utc)
        threshold = now + timedelta(seconds=wait_seconds)

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

        now = datetime.now(tz=timezone.utc)

        for e in Entries:
            if e["Id"] in self.in_flight:
                found_entries.append(e)
                in_flight_message = self.in_flight[e["Id"]]
                sec = int(e["VisibilityTimeout"])
                visible_at = now + timedelta(seconds=sec)
                updated_message = replace(in_flight_message, visible_at=visible_at)
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


@dataclass(frozen=True)
class MemoryMessage:
    """
    A mock class to mimic the AWS message

    Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
         services/sqs.html#SQS.Message
    """

    queue_impl: MemoryQueue = field(repr=False)

    # The message's contents (not URL-encoded).
    body: bytes = field()

    # Each message attribute consists of a Name, Type, and Value.
    message_attributes: dict[str, dict[str, str]] = field(default_factory=dict)

    # A map of the attributes requested in `` ReceiveMessage `` to their
    # respective values.
    attributes: dict[str, Any] = field(default_factory=dict)

    # Internal attribute which contains the execution time.
    visible_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    # A unique identifier for the message
    message_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    # The Message's receipt_handle identifier
    receipt_handle: str = field(default_factory=lambda: uuid.uuid4().hex)

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

        visible_at = datetime.now(tz=timezone.utc)
        if "DelaySeconds" in kwargs:
            delay_seconds_int = int(kwargs["DelaySeconds"])
            visible_at += timedelta(seconds=delay_seconds_int)

        return MemoryMessage(
            queue_impl, body, message_atttributes, attributes, visible_at
        )

    def change_visibility(self, VisibilityTimeout="0", **kwargs):
        if self.message_id in self.queue_impl.in_flight:
            now = datetime.now(tz=timezone.utc)
            sec = int(VisibilityTimeout)
            visible_at = now + timedelta(seconds=sec)
            updated_message = replace(self, visible_at=visible_at)
            self.queue_impl.in_flight[self.message_id] = updated_message
        else:
            logger.warning("Tried to change visibility of message not in flight")
