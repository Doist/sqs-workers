import logging
from typing import TYPE_CHECKING, Union

from sqs_workers import codecs
from sqs_workers.processor_mgr import ProcessorManagerProxy
from sqs_workers.shutdown_policies import NEVER_SHUTDOWN

DEFAULT_MESSAGE_GROUP_ID = "default"

if TYPE_CHECKING:
    from sqs_workers import MemoryEnv, SQSEnv


logger = logging.getLogger(__name__)


class GenericQueue(ProcessorManagerProxy):
    def __init__(self, env, name):
        # type: (Union[SQSEnv, MemoryEnv], str) -> None
        self.env = env
        self.name = name

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

    def get_queue(self):
        """
        Helper function to return queue object.
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
