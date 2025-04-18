from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class BatchProcessingResult:
    def __init__(self, queue_name: str, succeeded=None, failed=None):
        self.queue_name = queue_name
        self.succeeded = succeeded or []
        self.failed = failed or []

    def update_with_message(self, message: Any, success: bool):
        """Update processing result with a message."""
        if success:
            self.succeeded.append(message)
        else:
            self.failed.append(message)

    def succeeded_count(self) -> int:
        return len(self.succeeded)

    def failed_count(self) -> int:
        return len(self.failed)

    def total_count(self) -> int:
        return self.succeeded_count() + self.failed_count()

    def __repr__(self) -> str:
        return f"<BatchProcessingResult/{self.queue_name}/{self.succeeded_count()}/{self.failed_count()}>"


class RedrivePolicy:
    """
    Redrive Policy for SQS queues.

    See for more details:
    https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
    """

    def __init__(self, sqs_env, dead_letter_queue_name, max_receive_count):
        self.sqs_env = sqs_env
        self.dead_letter_queue_name = dead_letter_queue_name
        self.max_receive_count = max_receive_count

    def __json__(self) -> str:
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


def get_job_name(message) -> str | None:
    attrs = message.message_attributes or {}
    return (attrs.get("JobName") or {}).get("StringValue")
