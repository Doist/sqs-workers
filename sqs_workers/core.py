import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class BatchProcessingResult(object):
    succeeded = None
    failed = None

    def __init__(self, queue_name, succeeded=None, failed=None):
        self.queue_name = queue_name
        self.succeeded = succeeded or []
        self.failed = failed or []

    def update_with_message(self, message, success):
        # type: (Any, bool) -> None
        """
        Update processing result with a message.
        """
        if success:
            self.succeeded.append(message)
        else:
            self.failed.append(message)

    def succeeded_count(self):
        return len(self.succeeded)

    def failed_count(self):
        return len(self.failed)

    def total_count(self):
        return self.succeeded_count() + self.failed_count()

    def __repr__(self):
        return "<BatchProcessingResult/%s/%s/%s>" % (
            self.queue_name,
            self.succeeded_count(),
            self.failed_count(),
        )


class RedrivePolicy(object):
    """
    Redrive Policy for SQS queues

    See for more details:
    https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
    """

    def __init__(self, sqs_env, dead_letter_queue_name, max_receive_count):
        self.sqs_env = sqs_env
        self.dead_letter_queue_name = dead_letter_queue_name
        self.max_receive_count = max_receive_count

    def __json__(self):
        queue = self.sqs_env.sqs_resource.get_queue_by_name(
            QueueName=self.sqs_env.get_sqs_queue_name(self.dead_letter_queue_name)
        )
        target_arn = queue.attributes["QueueArn"]
        # Yes, it's double-encoded JSON :-/
        return json.dumps(
            {
                "deadLetterTargetArn": target_arn,
                "maxReceiveCount": str(self.max_receive_count),
            }
        )


def get_job_name(message):
    attrs = message.message_attributes or {}
    return (attrs.get("JobName") or {}).get("StringValue")
