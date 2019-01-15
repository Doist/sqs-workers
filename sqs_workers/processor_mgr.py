import logging
import warnings
from collections import defaultdict

from sqs_workers import processors
from sqs_workers.backoff_policies import DEFAULT_BACKOFF
from sqs_workers.codecs import DEFAULT_CONTENT_TYPE
from sqs_workers.processors import DEFAULT_CONTEXT_VAR
from sqs_workers.utils import adv_bind_arguments

logger = logging.getLogger(__name__)


class ProcessorManagerProxy(object):

    processors = None  # type: "ProcessorManager"

    def processor(self,
                  queue_name,
                  job_name,
                  pass_context=False,
                  context_var=DEFAULT_CONTEXT_VAR,
                  backoff_policy=None):
        """
        Decorator to assign processor to handle jobs with the name job_name
        from the queue queue_name

        Usage example:


        @sqs.processor('q1', 'say_hello')
        def say_hello(name):
            print("Hello, " + name)

        Then you can add messages to the queue by calling ".delay" attribute
        of a newly created object:

        >>> say_hello.delay(name='John')

        Here, instead of calling function locally, it will be pushed to the
        queue (essentially, mimics the non-blocking call of the function).

        If you still want to call function locally, you can call

        >>> say_hello(name='John')
        """

        def fn(processor):
            return self.processors.connect(queue_name, job_name, processor,
                                           pass_context, context_var,
                                           backoff_policy)

        return fn

    def batch_processor(self,
                        queue_name,
                        job_name,
                        pass_context=False,
                        context_var=DEFAULT_CONTEXT_VAR,
                        backoff_policy=None):
        """
        Decorator to assign a batch processor to handle jobs with the name
        job_name from the queue queue_name. The batch processor has to be a
        function which accepts the only argument "jobs".

        "jobs" is a list of properly decoded dicts

        Usage example:


        @sqs.processors.connect('q1', 'batch_say_hello')
        def batch_say_hello(jobs):
            names = ', '.join([job['name'] for job in jobs])
            print("Hello, " + names)

        Then you can add messages to the queue by calling ".delay" attribute
        of a newly created object:

        >>> batch_say_hello.delay(name='John')

        Here, instead of calling function locally, it will be pushed to the
        queue (essentially, mimics the non-blocking call of the function).

        If you still want to call function locally, you can call

        >>> batch_say_hello(name='John')
        """

        def fn(processor):
            return self.processors.connect_batch(queue_name, job_name,
                                                 processor, pass_context,
                                                 context_var, backoff_policy)

        return fn


class ProcessorManager(object):
    def __init__(
            self,
            sqs_env,
            backoff_policy=DEFAULT_BACKOFF,
            processor_maker=processors.Processor,
            batch_processor_maker=processors.BatchProcessor,
            fallback_processor_maker=processors.FallbackProcessor,
    ):
        self.sqs_env = sqs_env
        self.processors = defaultdict(lambda: {})
        self.backoff_policy = backoff_policy
        self.processor_maker = processor_maker
        self.batch_processor_maker = batch_processor_maker
        self.fallback_processor_maker = fallback_processor_maker
        self.processors = defaultdict(lambda: {})

    def connect(self,
                queue_name,
                job_name,
                processor,
                pass_context=False,
                context_var=DEFAULT_CONTEXT_VAR,
                backoff_policy=None):
        """
        Assign processor (a function) to handle jobs with the name job_name
        from the queue queue_name
        """
        extra = {
            'queue_name': queue_name,
            'job_name': job_name,
            'processor_name': processor.__module__ + '.' + processor.__name__,
        }
        logger.debug(
            'Connect {queue_name}.{job_name} to '
            'processor {processor_name}'.format(**extra),
            extra=extra)
        self.processors[queue_name][job_name] = self.processor_maker(
            self.sqs_env, queue_name, job_name, processor, pass_context, context_var,
            backoff_policy or self.backoff_policy)
        return AsyncTask(self.sqs_env, queue_name, job_name, processor)

    def connect_batch(self,
                      queue_name,
                      job_name,
                      processor,
                      pass_context=False,
                      context_var=DEFAULT_CONTEXT_VAR,
                      backoff_policy=None):
        """
        Assign a batch processor (function) to handle jobs with the name
        job_name from the queue queue_name
        """
        extra = {
            'queue_name': queue_name,
            'job_name': job_name,
            'processor_name': processor.__module__ + '.' + processor.__name__,
        }
        logger.debug(
            'Connect {queue_name}.{job_name} to '
            'batch processor {processor_name}'.format(**extra),
            extra=extra)
        self.processors[queue_name][job_name] = self.batch_processor_maker(
            self.sqs_env, queue_name, job_name, processor, pass_context, context_var,
            backoff_policy or self.backoff_policy)
        return AsyncBatchTask(self.sqs_env, queue_name, job_name, processor)

    def copy(self, src_queue, dst_queue):
        """
        Copy processors from src_queue to dst_queue (both queues identified
        by their names). Can be helpful to process dead-letter queue with
        processors from the main queue.

        Usage example.

        sqs = SQSEnv()
        ...
        sqs.processors.copy('foo', 'foo_dead')
        sqs.process_queue("foo_dead", shutdown_policy=IdleShutdown(10))

        Here the queue "foo_dead" will be processed with processors from the
        queue "foo".
        """
        for job_name, processor in self.processors[src_queue].items():
            self.processors[dst_queue][job_name] = processor.copy(
                queue_name=dst_queue)

    def get(self, queue_name, job_name):
        """
        Helper function to return a processor for the queue
        """
        processor = self.processors[queue_name].get(job_name)
        if processor is None:
            processor = self.fallback_processor_maker(self.sqs_env, queue_name,
                                                      job_name)
            self.processors[queue_name][job_name] = processor
        return processor


class AsyncTask(object):
    def __init__(self, sqs_env, queue_name, job_name, processor):
        self.sqs_env = sqs_env
        self.queue_name = queue_name
        self.job_name = job_name
        self.processor = processor
        self.__doc__ = processor.__doc__

    def __call__(self, *args, **kwargs):
        warnings.warn(
            'Async task {0.queue_name}.{0.job_name} called synchronously'.
            format(self))
        return self.processor(*args, **kwargs)

    def __repr__(self):
        return '<%s %s.%s>' % (self.__class__.__name__, self.queue_name,
                               self.job_name)

    def delay(self, *args, **kwargs):
        _content_type = kwargs.pop('_content_type', DEFAULT_CONTENT_TYPE)
        _delay_seconds = kwargs.pop('_delay_seconds', None)
        _deduplication_id = kwargs.pop('_deduplication_id', None)
        _group_id = kwargs.pop('_group_id', None)
        kwargs = adv_bind_arguments(self.processor, args, kwargs)
        return self.sqs_env.add_job(
            self.queue_name,
            self.job_name,
            _content_type=_content_type,
            _delay_seconds=_delay_seconds,
            _deduplication_id=_deduplication_id,
            _group_id=_group_id,
            **kwargs)


class AsyncBatchTask(AsyncTask):
    def __call__(self, **kwargs):
        warnings.warn(
            'Async task {0.queue_name}.{0.job_name} called synchronously'.
            format(self))
        return self.processor([kwargs])

    def delay(self, **kwargs):
        # unlike AsyncTask, we cannot validate or bind arguments, because
        # processor always accepts the list of dicts, so we just pass kwargs
        # as is
        return self.sqs_env.add_job(self.queue_name, self.job_name, **kwargs)
