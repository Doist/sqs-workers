import logging
import warnings
from typing import TYPE_CHECKING, Callable, Optional

import attr

from sqs_workers import codecs
from sqs_workers.backoff_policies import DEFAULT_BACKOFF, BackoffPolicy
from sqs_workers.context import SQSContext
from sqs_workers.utils import adv_validate_arguments

if TYPE_CHECKING:
    from sqs_workers.queue import GenericQueue

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

    queue = attr.ib()  # type: GenericQueue
    fn = attr.ib(default=None)  # type: Optional[Callable]
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
class RawProcessor(GenericProcessor):
    def process_message(self, message):
        extra = {"message_id": message.message_id, "queue_name": self.queue.name}
        logger.debug("Process {queue_name}.{message_id}".format(**extra), extra=extra)

        try:
            self.fn(message)
        except Exception:
            logger.exception(
                "Error while processing {queue_name}.{message_id}".format(**extra),
                extra=extra,
            )
            return False
        else:
            return True


@attr.s
class Processor(GenericProcessor):
    """
    Processor which calls its function for each incoming message.

    fn() accepts a single parameter "message" which is a decoded body of an
    SQS message
    """

    job_name = attr.ib(default="")  # type: str
    pass_context = attr.ib(default=False)  # type: bool
    context_var = attr.ib(default=DEFAULT_CONTEXT_VAR)  # type: str

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
