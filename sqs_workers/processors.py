import logging
import warnings
from typing import TYPE_CHECKING, Callable, Optional

import attr

from sqs_workers import codecs
from sqs_workers.backoff_policies import DEFAULT_BACKOFF, BackoffPolicy
from sqs_workers.context import SQSContext
from sqs_workers.utils import adv_validate_arguments

if TYPE_CHECKING:
    from sqs_workers.queue import SQSQueue

logger = logging.getLogger(__name__)

DEFAULT_CONTEXT_VAR = "context"


@attr.s
class GenericProcessor(object):
    """
    Base superclass for all types of processors. Accepts queue name and job
    name to take care of, and optionally the processing function fn(...)

    The way job messages are actually processed, is defined in the
    process_batch() function which has to be re-defined in a subclass.

    Notes on the process_batch implementation.

    - job_messages of process_batch() is a list of SQS message objects
    http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#message
    - The function must return two lists:
        - successfully processed messages
        - failed messages
    - The function is not supposed to die with any exception, instead the
      error has to be caught and logged, and the message has to be excluded
      from the list of successfully processed messages
    """

    queue = attr.ib()  # type: SQSQueue
    fn = attr.ib(default=None)  # type: Optional[Callable]
    pass_context = attr.ib(default=False)  # type: bool
    context_var = attr.ib(default=DEFAULT_CONTEXT_VAR)  # type: str
    backoff_policy = attr.ib(default=DEFAULT_BACKOFF)  # type: BackoffPolicy

    def process_message(self, message):
        # type: (...) -> bool
        """
        Process single message. Return True if processing succeeded.
        """
        raise NotImplementedError("Must be implemented in subclasses")

    def copy(self, **kwargs):
        """
        Create a new instance of the processor, optionally updating
        arguments of the constructor from update_kwargs
        """
        return attr.evolve(self, **kwargs)


@attr.s
class Processor(GenericProcessor):
    """
    Processor which calls its function for each incoming message.

    fn() accepts a single parameter "message" which is a decoded body of an
    SQS message
    """

    job_name = attr.ib(default="")  # type: str

    def process_message(self, message):
        extra = {
            "message_id": message.message_id,
            "queue_name": self.queue.name,
            "job_name": self.job_name,
        }
        logger.debug("Process {queue_name}.{job_name}".format(**extra), extra=extra)

        try:
            content_type = get_job_content_type(message)
            extra["job_content_type"] = content_type
            codec = codecs.get_codec(content_type)
            job_kwargs = codec.deserialize(message.body)
            job_context = get_job_context(message, codec)
            self.process(job_kwargs, job_context)
        except Exception:
            logger.exception(
                "Error while processing {queue_name}.{job_name}".format(**extra),
                extra=extra,
            )
            return False
        else:
            return True

    def process(self, job_kwargs, job_context):
        effective_kwargs = job_kwargs.copy()
        if self.pass_context:
            effective_kwargs[self.context_var] = job_context
        return call_handler(self.fn, effective_kwargs)


@attr.s
class FallbackProcessor(GenericProcessor):
    """
    Processor which is used by default when none of the processors is attached
    explicitly
    """

    job_name = attr.ib(default="")  # type: str

    def process_message(self, message):
        warnings.warn(
            "Error while processing {}.{}".format(self.queue.name, self.job_name)
        )
        return False


@attr.s
class DeadLetterProcessor(GenericProcessor):
    """
    Generic processor which can be used for the dead-letter queue to push back
    messages to the main queue. Has opinion on how queues are organized
    and uses some hard-coded options.

    It is supposed to process queues "something_dead" which is supposed to be
    a configured dead-letter queue for "something". In case of FIFO queues
    names would be "something_dead.fifo" for "something.fifo".

    While processing the queue, the processor takes every message and push it
    back to the queue "something" (or something.fifo) with a hard-coded delay
    of 1 second.

    If the queue name does't end with "_dead" or "_dead.fifo", the
    DeadLetterProcessor behaves like generic FallbackProcessor: shows the error
    message and keep message in the queue. It's made to prevent from creating
    infinite loops when the message from the dead letter queue is pushed back
    to the same queue, then immediately processed by the same processor again,
    etc.

    Usage example:

    >>> env = SQSEnv(fallback_processor_maker=DeadLetterProcessor)
    >>> env.process_queue("foo_dead", shutdown_policy=IdleShutdown(10))

    This code takes all the messages in foo_dead queue and push them back to
    the foo queue. Then it waits 10 seconds to ensure no new messages appear,
    and quit.
    """

    def process_message(self, message):
        if not is_deadletter(self.queue.name):
            warnings.warn("Error while processing {}".format(self.queue.name))
            return False

        self.push_back_message(message)
        return True

    def push_back_message(self, message):
        raise NotImplementedError()
        target_queue_name = get_deadletter_upstream_name(self.queue.name)
        target_queue = self.queue.env.queue(target_queue_name)

        content_type = message.message_attributes["ContentType"]["StringValue"]
        job_context = message.message_attributes["JobContext"]["StringValue"]
        if target_queue_name.endswith(".fifo"):
            deduplication_id = message.attributes["MessageDeduplicationId"]
            group_id = message.attributes["MessageGroupId"]
            # FIFO queues don't allow to set delay_seconds
            delay_seconds = None
        else:
            deduplication_id = None
            group_id = None
            delay_seconds = 1
        logger.debug(
            "Push back dead letter job {}.{}".format(self.queue.name, self.job_name),
            extra={
                "target_queue_name": target_queue_name,
                "queue_name": self.queue.name,
                "job_name": self.job_name,
            },
        )
        target_queue.add_raw_job(
            self.job_name,
            message.body,
            job_context,
            content_type,
            delay_seconds,
            deduplication_id,
            group_id,
        )


def is_deadletter(queue_name):
    """
    Helper function which returns if queue has valid deadletter name
    (that is, ends with "_dead" or "_dead.fifo")
    """
    return queue_name.endswith("_dead") or queue_name.endswith("_dead.fifo")


def get_deadletter_upstream_name(queue_name):
    """
    Return upstream name for deadletter queue name: "foo" for "foo_dead"
    and "foo.fifo" for "foo_dead.fifo". Raise RuntimeError if queue doesn't
    have a valid deadletter name.
    """
    if queue_name.endswith("_dead"):
        return queue_name[: -len("_dead")]
    if queue_name.endswith("_dead.fifo"):
        return queue_name[: -len("_dead.fifo")] + ".fifo"
    raise RuntimeError("Not a deadletter queue")


def get_job_content_type(job_message):
    attrs = job_message.message_attributes or {}
    return (attrs.get("ContentType") or {}).get("StringValue")


def get_job_context(job_message, codec):
    attrs = job_message.message_attributes or {}
    serialized = (attrs.get("JobContext") or {}).get("StringValue")
    if not serialized:
        return SQSContext()
    deserialized = codec.deserialize(serialized)
    return SQSContext.from_dict(deserialized)


def call_handler(fn, kwargs):
    try:
        handler_args, handler_kwargs = adv_validate_arguments(fn, [], kwargs)
    except TypeError:
        # it may happen, if "fn" is not a function (but
        # a mock object, for example)
        handler_args, handler_kwargs = [], kwargs
    fn(*handler_args, **handler_kwargs)
