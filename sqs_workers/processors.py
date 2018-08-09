import logging
import warnings

from sqs_workers.utils import adv_validate_arguments
from sqs_workers import codecs

logger = logging.getLogger(__name__)


class GenericProcessor(object):
    """
    Base superclass for all types of processors. Accepts queue name and job
    name to take care of, and optinally the processing function fn(...)

    The way job messages are actually processed, is defined in the
    process_batch() function which has to be re-defined in a subclass.

    Notes on the process_batch implementation.

    - job_messages of process_batch() is a list of SQS message objects
    http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#message
    - The function must return a list of successfully processed messages.
    - The function is not supposed to die with any exception, instead the
      error has to be caught and logged, and the message has to be excluded
      from the list of successfully processed messages
    """

    def __init__(self, queue_name, job_name, fn=None):
        self.queue_name = queue_name
        self.job_name = job_name
        self.fn = fn

    def process_batch(self, job_messages):
        raise NotImplementedError("Must be implemented in subclasses")


class Processor(GenericProcessor):
    """
    Processor which calls its function for each incoming message.

    fn() accepts a single parameter "message" which is a decoded body of an
    SQS message
    """

    def process_batch(self, job_messages):
        ret = []
        for message in job_messages:
            extra = {
                'message_id': message.message_id,
                'queue_name': self.queue_name,
                'job_name': self.job_name,
            }
            logger.debug(
                'Process {queue_name}.{job_name}'.format(**extra), extra=extra)

            try:
                content_type = get_job_content_type(message)
                extra['job_content_type'] = content_type
                codec = codecs.get_codec(content_type)
                job_kwargs = codec.deserialize(message.body)
                call_handler(self.fn, job_kwargs)
            except Exception:
                logger.exception(
                    'Error while processing {queue_name}.{job_name}'.format(
                        **extra),
                    extra=extra)
                continue

            ret.append(message)
        return ret


class BatchProcessor(GenericProcessor):
    """
    Processor which calls its function for the entire batch of incoming.

    The function is called for the list of decoded message bodies
    """

    def process_batch(self, job_messages):
        if job_messages:
            extra = {
                'job_count': len(job_messages),
                'queue_name': self.queue_name,
                'job_name': self.job_name,
            }
            logger.debug(
                'Process {job_count} messages '
                'to {queue_name}.{job_name}'.format(**extra),
                extra=extra)
            try:
                jobs = self.decode_messages(job_messages)
                self.fn(jobs)
            except Exception:
                logger.exception(
                    'Error while processing {job_count} messages '
                    'to {queue_name}.{job_name}'.format(**extra),
                    extra=extra)
                return []
            else:
                return job_messages

    def decode_messages(self, job_messages):
        jobs = []
        for message in job_messages:
            content_type = get_job_content_type(message)
            codec = codecs.get_codec(content_type)
            job = codec.deserialize(message.body)
            jobs.append(job)
        return jobs


class FallbackProcessor(GenericProcessor):
    """
    Processor which is used by default when none of the processors is attached
    explicity
    """

    def process_batch(self, job_messages):
        warnings.warn('Error while processing {}.{}'.format(
            self.queue_name, self.job_name))
        return []


def get_job_content_type(job_message):
    attrs = job_message.message_attributes or {}
    return (attrs.get('ContentType') or {}).get('StringValue')


def call_handler(fn, kwargs):
    try:
        handler_args, handler_kwargs = adv_validate_arguments(fn, [], kwargs)
    except TypeError:
        # it may happen, if "fn" is not a function (but
        # a mock object, for example)
        handler_args, handler_kwargs = [], kwargs
    fn(*handler_args, **handler_kwargs)
