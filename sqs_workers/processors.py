import logging
from functools import partial
from typing import TYPE_CHECKING, Callable, Optional

import attr

from sqs_workers import codecs
from sqs_workers.context import SQSContext
from sqs_workers.utils import validate_arguments

if TYPE_CHECKING:
    from sqs_workers.queue import GenericQueue

logger = logging.getLogger(__name__)

DEFAULT_CONTEXT_VAR = "context"


@attr.s
class Processor(object):
    """
    Processor which calls its function for each incoming message.

    fn() accepts a single parameter "message" which is a decoded body of an
    SQS message
    """

    queue: "GenericQueue" = attr.ib()
    fn: Optional[Callable] = attr.ib(default=None)
    job_name: str = attr.ib(default="")
    pass_context: bool = attr.ib(default=False)
    context_var: str = attr.ib(default=DEFAULT_CONTEXT_VAR)

    @classmethod
    def maker(cls, **kwargs):
        return partial(cls, **kwargs)

    def process_message(self, message):
        """
        Takes an individual message and sends it to the decorated call handler function.
        """
        extra = {
            "message_id": message.message_id,
            "queue_name": self.queue.name,
            "job_name": self.job_name,
        }
        logger.debug("Process %s.%s", self.queue.name, self.job_name, extra=extra)

        try:
            content_type, job_kwargs, job_context = self.deserialize_message(message)
            extra["job_content_type"] = content_type
            self.process(job_kwargs, job_context)
        except Exception:
            logger.exception(
                "Error while processing %s.%s",
                self.queue.name,
                self.job_name,
                extra=extra,
            )
            return False
        else:
            return True

    def process_messages(self, messages):
        """
        Take a list of messages and send them in a batch
        to the decorated call handler function.
        """
        message_ids = [m.message_id for m in messages]
        extra = {
            "message_ids": message_ids,
            "queue_name": self.queue.name,
            "job_name": self.job_name,
        }
        logger.debug(
            "Processing batch for %s.%s",
            self.queue.name,
            self.job_name,
            extra=extra,
        )

        try:
            self.process_batch(messages)
        except Exception:
            logger.exception(
                "Error while processing batch for %s.%s",
                self.queue.name,
                self.job_name,
                extra=extra,
            )
            return False
        else:
            return True

    def process(self, job_kwargs, job_context):
        effective_kwargs = job_kwargs.copy()
        if self.pass_context:
            effective_kwargs[self.context_var] = job_context
        return call_handler(self.fn, [], effective_kwargs)

    def process_batch(self, messages):
        deserialized_messages = []
        for message in messages:
            _, job_kwargs, job_context = self.deserialize_message(message)
            if self.pass_context:
                job_kwargs[self.context_var] = job_context
            deserialized_messages.append(job_kwargs)

        call_handler(self.fn, [deserialized_messages], {})

    def copy(self, **kwargs):
        """
        Create a new instance of the processor, optionally updating
        arguments of the constructor from update_kwargs
        """
        return attr.evolve(self, **kwargs)

    @staticmethod
    def deserialize_message(message):
        content_type = get_job_content_type(message)
        codec = codecs.get_codec(content_type)
        job_kwargs = codec.deserialize(message.body)
        job_context = get_job_context(message, codec)
        return content_type, job_kwargs, job_context


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


def call_handler(fn, args, kwargs):
    try:
        handler_args, handler_kwargs = validate_arguments(fn, args, kwargs)
    except TypeError:
        # it may happen, if "fn" is not a function (but
        # a mock object, for example)
        handler_args, handler_kwargs = args, kwargs
    return fn(*handler_args, **handler_kwargs)
