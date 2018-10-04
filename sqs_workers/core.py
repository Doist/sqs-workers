import json
import logging
import multiprocessing
import warnings
from collections import defaultdict
from itertools import groupby

import boto3

from sqs_workers import codecs, processors
from sqs_workers.backoff_policies import DEFAULT_BACKOFF
from sqs_workers.shutdown_policies import NEVER_SHUTDOWN, NeverShutdown
from sqs_workers.utils import adv_bind_arguments

logger = logging.getLogger(__name__)

DEFAULT_CONTENT_TYPE = 'pickle'


class BatchProcessingResult(object):
    succeeded = None
    failed = None

    def __init__(self, succeeded=None, failed=None):
        self.succeeded = succeeded or []
        self.failed = failed or []

    def update(self, succeeded, failed):
        self.succeeded += succeeded
        self.failed += failed

    def succeeded_count(self):
        return len(self.succeeded)

    def failed_count(self):
        return len(self.failed)

    def total_count(self):
        return self.succeeded_count() + self.failed_count()

    def __repr__(self):
        return '<BatchProcessingResult/%s/%s>' % (self.succeeded_count(),
                                                  self.failed_count())


class SQSEnv(object):
    def __init__(self,
                 session=boto3,
                 queue_prefix='',
                 backoff_policy=DEFAULT_BACKOFF):
        """
        Initialize SQS environment with boto3 session
        """
        self.session = session
        self.sqs_client = session.client('sqs')
        self.sqs_resource = session.resource('sqs')
        self.processors = defaultdict(lambda: {})
        self.queue_prefix = queue_prefix
        self.backoff_policy = backoff_policy

        # internal mapping from queue names to queue objects
        self.queue_mapping_cache = {}

    def create_standard_queue(self,
                              queue_name,
                              message_retention_period=None,
                              visibility_timeout=None,
                              redrive_policy=None):
        """
        Create a new standard queue
        """
        attrs = {}
        kwargs = {
            'QueueName': self.get_sqs_queue_name(queue_name),
            'Attributes': attrs,
        }
        if message_retention_period is not None:
            attrs['MessageRetentionPeriod'] = str(message_retention_period)
        if visibility_timeout is not None:
            attrs['VisibilityTimeout'] = str(visibility_timeout)
        if redrive_policy is not None:
            attrs['RedrivePolicy'] = redrive_policy.__json__()
        ret = self.sqs_client.create_queue(**kwargs)
        return ret['QueueUrl']

    def create_fifo_queue(self,
                          queue_name,
                          content_based_deduplication=False,
                          message_retention_period=None,
                          visibility_timeout=None,
                          redrive_policy=None):
        """
        Create a new FIFO queue. Note that queue name has to end with ".fifo"

        - "content_based_deduplication" turns on automatic content-based
          deduplication of messages in the queue

        - redrive_policy can be None or an object, generated with
          redrive_policy() method of SQS. In the latter case if defines the
          way failed messages are processed.
        """
        attrs = {
            'FifoQueue': 'true',
        }
        kwargs = {
            'QueueName': self.get_sqs_queue_name(queue_name),
            'Attributes': attrs,
        }
        if content_based_deduplication:
            attrs['ContentBasedDeduplication'] = 'true'
        if message_retention_period is not None:
            attrs['MessageRetentionPeriod'] = str(message_retention_period)
        if visibility_timeout is not None:
            attrs['VisibilityTimeout'] = str(visibility_timeout)
        if redrive_policy is not None:
            attrs['RedrivePolicy'] = redrive_policy.__json__()
        ret = self.sqs_client.create_queue(**kwargs)
        return ret['QueueUrl']

    def purge_queue(self, queue_name):
        """
        Remove all messages from the queue
        """
        self.get_queue(queue_name).purge()

    def delete_queue(self, queue_name):
        """
        Delete the queue
        """
        self.get_queue(queue_name).delete()

    def connect_processor(self,
                          queue_name,
                          job_name,
                          processor,
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
        self.processors[queue_name][job_name] = processors.Processor(
            queue_name, job_name, processor, backoff_policy
            or self.backoff_policy)
        return AsyncTask(self, queue_name, job_name, processor)

    def processor(self, queue_name, job_name, backoff_policy=None):
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
            return self.connect_processor(queue_name, job_name, processor,
                                          backoff_policy)

        return fn

    def connect_batch_processor(self,
                                queue_name,
                                job_name,
                                processor,
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
        self.processors[queue_name][job_name] = processors.BatchProcessor(
            queue_name, job_name, processor, backoff_policy
            or self.backoff_policy)
        return AsyncBatchTask(self, queue_name, job_name, processor)

    def batch_processor(self, queue_name, job_name, backoff_policy=None):
        """
        Decorator to assign a batch processor to handle jobs with the name
        job_name from the queue queue_name. The batch processor has to be a
        function which accepts the only argument "jobs".

        "jobs" is a list of properly decoded dicts

        Usage example:


        @sqs.batch_processor('q1', 'batch_say_hello')
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
            return self.connect_batch_processor(queue_name, job_name,
                                                processor, backoff_policy)

        return fn

    def add_job(self,
                queue_name,
                job_name,
                _content_type=DEFAULT_CONTENT_TYPE,
                _delay_seconds=None,
                **job_kwargs):
        """
        Add job to the queue. The body of the job will be converted to the text
        with one of the codes (by default it's "pickle")
        """
        codec = codecs.get_codec(_content_type)
        message_body = codec.serialize(job_kwargs)
        return self.add_raw_job(queue_name, job_name, message_body,
                                _content_type, _delay_seconds)

    def add_raw_job(self, queue_name, job_name, message_body, content_type,
                    delay_seconds):
        """
        Low-level function to put message to the queue
        """
        queue = self.get_queue(queue_name)
        kwargs = {
            'MessageBody': message_body,
            'MessageAttributes': {
                'ContentType': {
                    'StringValue': content_type,
                    'DataType': 'String',
                },
                'JobName': {
                    'StringValue': job_name,
                    'DataType': 'String',
                },
            },
        }
        if delay_seconds is not None:
            kwargs['DelaySeconds'] = int(delay_seconds)
        ret = queue.send_message(**kwargs)
        return ret['MessageId']

    def process_queues(self,
                       queue_names=None,
                       shutdown_policy_maker=NeverShutdown):
        """
        Use multiprocessing to process multiple queues at once. If queue names
        are not set, process all known queues

        shutdown_policy_maker is an optional callable which doesn't accept any
        arguments and create a new shutdown policy for each queue.

        Can looks somewhat like this:

            lambda: IdleShutdown(idle_seconds=10)
        """
        if not queue_names:
            queue_names = self.get_all_known_queues()
        processes = []
        for queue_name in queue_names:
            p = multiprocessing.Process(
                target=self.process_queue,
                kwargs={
                    'queue_name': queue_name,
                    'shutdown_policy': shutdown_policy_maker(),
                })
            p.start()
            processes.append(p)
        for p in processes:
            p.join()

    def drain_queue(self, queue_name, wait_seconds=0):
        """
        Delete all messages from the queue without calling purge()
        """
        queue = self.get_queue(queue_name)
        deleted_count = 0
        while True:
            messages = self.get_raw_messages(queue_name, wait_seconds)
            if not messages:
                break
            entries = [{
                'Id': msg.message_id,
                'ReceiptHandle': msg.receipt_handle
            } for msg in messages]
            queue.delete_messages(Entries=entries)
            deleted_count += len(messages)
        return deleted_count

    def process_queue(self,
                      queue_name,
                      shutdown_policy=NEVER_SHUTDOWN,
                      wait_second=10):
        """
        Run worker to process one queue in the infinite loop
        """
        logger.debug(
            'Start processing queue {}'.format(queue_name),
            extra={
                'queue_name': queue_name,
                'wait_seconds': wait_second,
                'shutdown_policy': repr(shutdown_policy),
            })
        while True:
            result = self.process_batch(queue_name, wait_seconds=wait_second)
            shutdown_policy.update_state(result)
            if shutdown_policy.need_shutdown():
                logger.debug(
                    'Stop processing queue {}'.format(queue_name),
                    extra={
                        'queue_name': queue_name,
                        'wait_seconds': wait_second,
                        'shutdown_policy': repr(shutdown_policy),
                    })
                break

    def process_batch(self, queue_name, wait_seconds=0):
        # type: (str, int) -> BatchProcessingResult
        """
        Process a batch of messages from the queue (10 messages at most), return
        the number of successfully processed messages, and exit
        """
        queue = self.get_queue(queue_name)
        messages = self.get_raw_messages(queue_name, wait_seconds)
        result = BatchProcessingResult()
        for job_name, job_messages in self.group_messages(messages):
            processor = self.get_processor(queue_name, job_name)
            succeeded, failed = processor.process_batch(job_messages)
            result.update(succeeded, failed)
            if succeeded:
                entries = [{
                    'Id': msg.message_id,
                    'ReceiptHandle': msg.receipt_handle
                } for msg in succeeded]
                queue.delete_messages(Entries=entries)
            if failed:
                entries = [{
                    'Id':
                    msg.message_id,
                    'ReceiptHandle':
                    msg.receipt_handle,
                    'VisibilityTimeout':
                    processor.backoff_policy.get_visibility_timeout(msg),
                } for msg in failed]
                queue.change_message_visibility_batch(Entries=entries)
        return result

    def get_raw_messages(self, queue_name, wait_seconds):
        """
        Return raw messages from the queue, addressed by its name
        """
        kwargs = {
            'WaitTimeSeconds': wait_seconds,
            'MaxNumberOfMessages': 10,
            'MessageAttributeNames': ['All'],
            'AttributeNames': ['All'],
        }
        queue = self.get_queue(queue_name)
        return queue.receive_messages(**kwargs)

    def get_queue(self, queue_name):
        """
        Helper function to return queue object by name
        """
        if queue_name not in self.queue_mapping_cache:
            queue = self.sqs_resource.get_queue_by_name(
                QueueName=self.get_sqs_queue_name(queue_name))
            self.queue_mapping_cache[queue_name] = queue
        return self.queue_mapping_cache[queue_name]

    def get_all_known_queues(self):
        resp = self.sqs_client.list_queues(
            **{
                'QueueNamePrefix': self.queue_prefix,
            })
        if 'QueueUrls' not in resp:
            return []
        urls = resp['QueueUrls']
        ret = []
        for url in urls:
            sqs_name = url.rsplit('/', 1)[-1]
            ret.append(sqs_name[len(self.queue_prefix):])
        return ret

    def get_processor(self, queue_name, job_name):
        """
        Helper function to return a processor for the queue
        """
        processor = self.processors[queue_name].get(job_name)
        if processor is None:
            processor = processors.FallbackProcessor(queue_name, job_name)
            self.processors[queue_name][job_name] = processor
        return processor

    def group_messages(self, messages):
        messages = sorted(messages, key=get_job_name)
        for job_name, job_messages in groupby(messages, key=get_job_name):
            yield job_name, list(job_messages)

    def get_sqs_queue_name(self, queue_name):
        """
        Take "high-level" (user-visible) queue name and return SQS
        ("low level") name by simply prefixing it. Used to create namespaces
        for different environments (development, staging, production, etc)
        """
        return '{}{}'.format(self.queue_prefix, queue_name)

    def redrive_policy(self, dead_letter_queue_name, max_receive_count):
        return RedrivePolicy(self, dead_letter_queue_name, max_receive_count)


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
        kwargs = adv_bind_arguments(self.processor, args, kwargs)
        return self.sqs_env.add_job(
            self.queue_name,
            self.job_name,
            _content_type=_content_type,
            _delay_seconds=_delay_seconds,
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
            QueueName=self.sqs_env.get_sqs_queue_name(
                self.dead_letter_queue_name))
        target_arn = queue.attributes['QueueArn']
        # Yes, it's double-encoded JSON :-/
        return json.dumps({
            'deadLetterTargetArn': target_arn,
            'maxReceiveCount': str(self.max_receive_count),
        })


def get_job_name(message):
    attrs = message.message_attributes or {}
    return (attrs.get('JobName') or {}).get('StringValue')
