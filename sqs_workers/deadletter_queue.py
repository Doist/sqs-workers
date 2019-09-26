import logging
from functools import partial
from typing import TYPE_CHECKING

import attr

from sqs_workers import RawQueue

if TYPE_CHECKING:
    from sqs_workers.queue import GenericQueue


logger = logging.getLogger(__name__)


@attr.s
class DeadLetterQueue(RawQueue):
    """
    Queue to push back messages to the upstream.

    While processing the queue, the processor takes every message and push it
    back to the upstream queue with a hard-coded delay of 1 second.

    Usage example:

    >>> from sqs_workers import JobQueue
    >>> from sqs_workers.shutdown_policies IdleShutdown
    >>> from sqs_workers.deadletter_queue import DeadLetterQueue
    >>> env = SQSEnv()
    >>> foo = env.queue("foo")
    >>> foo_dead = env.queue("foo_dead", DeadLetterQueue.maker(foo))
    >>> foo_dead.process_queue(shutdown_policy=IdleShutdown(10))

    This code takes all the messages in foo_dead queue and push them back to
    the foo queue. Then it waits 10 seconds to ensure no new messages appear,
    and quit.
    """

    upstream_queue = attr.ib(default=None)  # type: GenericQueue

    @classmethod
    def maker(cls, upstream_queue, **kwargs):
        processor = PushBackSender(upstream_queue)
        return partial(
            cls, processor=processor, upstream_queue=upstream_queue, **kwargs
        )


@attr.s
class PushBackSender(object):

    upstream_queue = attr.ib(default=None)  # type: GenericQueue

    def __call__(self, message):
        kwargs = {
            "MessageBody": message.body,
            "MessageAttributes": message.message_attributes or {},
        }

        if self.upstream_queue.name.endswith(".fifo"):
            kwargs.update(
                {
                    "MessageDeduplicationId": message.attributes[
                        "MessageDeduplicationId"
                    ],
                    "MessageGroupId": message.attributes["MessageGroupId"],
                }
            )
        else:
            kwargs.update({"DelaySeconds": 1})
        self.upstream_queue.get_queue().send_message(**kwargs)
