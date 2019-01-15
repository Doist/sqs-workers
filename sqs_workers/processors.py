import logging
import warnings

from sqs_workers.backoff_policies import DEFAULT_BACKOFF
from sqs_workers.context import SQSContext
from sqs_workers.utils import adv_validate_arguments
from sqs_workers import codecs

logger = logging.getLogger(__name__)

DEFAULT_CONTEXT_VAR = 'context'


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

    def __init__(self,
                 sqs_env,
                 queue_name,
                 job_name,
                 fn=None,
                 pass_context=False,
                 context_var=DEFAULT_CONTEXT_VAR,
                 backoff_policy=DEFAULT_BACKOFF):
        self.sqs_env = sqs_env
        self.queue_name = queue_name
        self.job_name = job_name
        self.fn = fn
        self.pass_context = pass_context
        self.context_var = context_var
        self.backoff_policy = backoff_policy

    def __repr__(self):
        if self.fn:
            fn_name = self.fn.__module__ + '.' + self.fn.__name__
        else:
            fn_name = ''
        return '{}({!r}, {!r}, {})'.format(
            self.__class__.__name__, self.queue_name, self.job_name, fn_name)

    def process_batch(self, job_messages):
        raise NotImplementedError("Must be implemented in subclasses")

    def copy(self, **kwargs):
        """
        Create a new instance of the processor, optionally updating
        arguments of the constructor from update_kwargs
        """
        init_kwargs = {
            'sqs_env': kwargs.get('sqq_env', self.sqs_env),
            'queue_name': kwargs.get('queue_name', self.queue_name),
            'job_name': kwargs.get('job_name', self.job_name),
            'fn': kwargs.get('fn', self.fn),
            'pass_context': kwargs.get('pass_context', self.pass_context),
            'context_var': kwargs.get('context_var', self.context_var),
            'backoff_policy': kwargs.get('backoff_policy',
                                         self.backoff_policy),
        }
        return self.__class__(**init_kwargs)


class Processor(GenericProcessor):
    """
    Processor which calls its function for each incoming message.

    fn() accepts a single parameter "message" which is a decoded body of an
    SQS message
    """

    def process_batch(self, job_messages):
        succeeded, failed = [], []
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
                job_context = get_job_context(message, codec)
                self.process(job_kwargs, job_context)
            except Exception:
                logger.exception(
                    'Error while processing {queue_name}.{job_name}'.format(
                        **extra),
                    extra=extra)
                failed.append(message)
            else:
                succeeded.append(message)
        return succeeded, failed

    def process(self, job_kwargs, job_context):
        effective_kwargs = job_kwargs.copy()
        if self.pass_context:
            effective_kwargs[self.context_var] = job_context
        return call_handler(self.fn, effective_kwargs)


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
                jobs, context = self.decode_messages(job_messages)
                self.process(jobs, context)
            except Exception:
                logger.exception(
                    'Error while processing {job_count} messages '
                    'to {queue_name}.{job_name}'.format(**extra),
                    extra=extra)
                return [], job_messages  # all failed
            else:
                return job_messages, []  # all succeeded

    def decode_messages(self, job_messages):
        jobs = []
        context = []
        for message in job_messages:
            content_type = get_job_content_type(message)
            codec = codecs.get_codec(content_type)
            job = codec.deserialize(message.body)
            jobs.append(job)
            context.append(get_job_context(message, codec))
        return jobs, context

    def process(self, jobs, context):
        if self.pass_context:
            return self.fn(jobs, **{self.context_var: context})
        return self.fn(jobs)


class FallbackProcessor(GenericProcessor):
    """
    Processor which is used by default when none of the processors is attached
    explicitly
    """

    def process_batch(self, job_messages):
        warnings.warn('Error while processing {}.{}'.format(
            self.queue_name, self.job_name))
        # return empty list for succeeded and failed messages
        # - the queue will not delete messages
        # - it will not put messages back to the queue either, so they
        #   automatically appear there on reaching the visibility timeout
        return [], []


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

    def process_batch(self, job_messages):
        if not self.is_dead():
            warnings.warn('Error while processing {}.{}'.format(
                self.queue_name, self.job_name))
            return [], []

        for message in job_messages:
            self.push_back_message(message)
        return job_messages, []  # all succeeded

    def push_back_message(self, message):
        queue_name = self.get_upstream_name()
        content_type = message.message_attributes['ContentType']['StringValue']
        job_context = message.message_attributes['JobContext']['StringValue']
        if queue_name.endswith('.fifo'):
            deduplication_id = message.message_attributes[
                'MessageDeduplicationId']['StringValue']
            group_id = message.message_attributes['MessageGroupId'][
                'StringValue']
        else:
            deduplication_id = None
            group_id = None
        logger.debug(
            'Push back dead letter job {}.{}'.format(self.queue_name,
                                                     self.job_name),
            extra={
                'target_queue_name': self.queue_name,
                'queue_name': self.queue_name,
                'job_name': self.job_name,
            })
        self.sqs_env.add_raw_job(queue_name, self.job_name, message.body,
                                 job_context, content_type, 1,
                                 deduplication_id, group_id)

    def is_dead(self):
        return (self.queue_name.endswith('_dead')
                or self.queue_name.endswith('_dead.fifo'))

    def get_upstream_name(self):
        if self.queue_name.endswith('_dead'):
            return self.queue_name[:-len('_dead')]
        if self.queue_name.endswith('_dead.fifo'):
            return self.queue_name[:-len('_dead.fifo')] + '.fifo'
        raise RuntimeError("Not a deadletter queue")


def get_job_content_type(job_message):
    attrs = job_message.message_attributes or {}
    return (attrs.get('ContentType') or {}).get('StringValue')


def get_job_context(job_message, codec):
    attrs = job_message.message_attributes or {}
    serialized = (attrs.get('JobContext') or {}).get('StringValue')
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
