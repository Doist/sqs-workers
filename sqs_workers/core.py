import logging
import json

import boto3
from collections import defaultdict
from itertools import groupby

import multiprocessing

from sqs_workers import processors, codecs
from sqs_workers.backoff_policies import DEFAULT_BACKOFF

logger = logging.getLogger(__name__)

DEFAULT_CONTENT_TYPE = 'pickle'


class SQSEnv(object):
    def __init__(self, session=boto3, queue_prefix='', backoff_policy=DEFAULT_BACKOFF):
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

    def create_standard_queue(self, queue_name, redrive_policy=None):
        """
        Create a new standard queue
        """
        kwargs = {
            'QueueName': self.get_sqs_queue_name(queue_name),
            'Attributes': {},
        }
        if redrive_policy is not None:
            kwargs['Attributes']['RedrivePolicy'] = redrive_policy.__json__()
        ret = self.sqs_client.create_queue(**kwargs)
        return ret['QueueUrl']

    def create_fifo_queue(self,
                          queue_name,
                          content_based_deduplication=False,
                          redrive_policy=None):
        """
        Create a new FIFO queue. Note that queue name has to end with ".fifo"

        - "content_based_deduplication" turns on automatic content-based
          deduplication of messages in the queue

        - redrive_policy can be None or an object, generated with
          redrive_policy() method of SQS. In the latter case if defines the
          way failed messages are processed.
        """
        kwargs = {
            'QueueName': self.get_sqs_queue_name(queue_name),
            'Attributes': {
                'FifoQueue': 'true',
            }
        }
        if content_based_deduplication:
            kwargs['Attributes']['ContentBasedDeduplication'] = 'true'
        if redrive_policy is not None:
            kwargs['Attributes']['RedrivePolicy'] = redrive_policy.__json__()
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

    def connect_processor(self, queue_name, job_name, processor, backoff_policy=None):
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
            queue_name, job_name, processor, backoff_policy or self.backoff_policy)
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
            return self.connect_processor(queue_name, job_name, processor, backoff_policy)

        return fn

    def connect_batch_processor(self, queue_name, job_name, processor, backoff_policy=None):
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
            queue_name, job_name, processor, backoff_policy or self.backoff_policy)
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
                **job_kwargs):
        """
        Add job to the queue. The body of the job will be converted to the text
        with one of the codes (by default it's "pickle")
        """
        codec = codecs.get_codec(_content_type)
        message_body = codec.serialize(job_kwargs)
        return self.add_raw_job(queue_name, job_name, message_body,
                                _content_type)

    def add_raw_job(self, queue_name, job_name, message_body, content_type):
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
        ret = queue.send_message(**kwargs)
        return ret['MessageId']

    def process_queues(self, queue_names=None):
        """
        Use multiprocessing to process multiple queues at once. If queue names
        are not set, process all known queues
        """
        if not queue_names:
            queue_names = self.get_all_known_queues()
        processes = []
        for queue_name in queue_names:
            p = multiprocessing.Process(
                target=self.process_queue,
                args=(queue_name, ))
            p.start()
            processes.append(p)
        for p in processes:
            p.join()

    def process_queue(self, queue_name):
        """
        Run worker to process one queue in the infinite loop
        """
        wait_seconds = 10
        while True:
            self.process_batch(queue_name, wait_seconds)

    def process_batch(self, queue_name, wait_seconds):
        """
        Process a batch of messages from the queue (10 messages at most), return
        the number of successfully processed messages, and exit
        :return:
        """
        queue = self.get_queue(queue_name)
        messages = self.get_raw_messages(queue_name, wait_seconds)
        succeeded_count = 0
        for job_name, job_messages in self.group_messages(messages):
            processor = self.get_processor(queue_name, job_name)
            succeeded, failed = processor.process_batch(job_messages)
            if succeeded:
                entries = [{
                    'Id': msg.message_id,
                    'ReceiptHandle': msg.receipt_handle
                } for msg in succeeded]
                queue.delete_messages(Entries=entries)
                succeeded_count += len(succeeded)
            if failed:
                entries = [{
                    'Id': msg.message_id,
                    'ReceiptHandle': msg.receipt_handle,
                    'VisibilityTimeout': processor.backoff_policy.get_visibility_timeout(msg),
                } for msg in failed]
                queue.change_message_visibility_batch(Entries=entries)
        return succeeded_count

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

    def __call__(self, **kwargs):
        return self.processor(**kwargs)

    def __repr__(self):
        return '<%s %s.%s>' % (self.__class__.__name__, self.queue_name,
                               self.job_name)

    def delay(self, **kwargs):
        self.sqs_env.add_job(self.queue_name, self.job_name, **kwargs)


class AsyncBatchTask(AsyncTask):
    def __call__(self, **kwargs):
        return self.processor([kwargs])


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
            'maxReceiveCount': self.max_receive_count,
        })


def get_job_name(message):
    attrs = message.message_attributes or {}
    return (attrs.get('JobName') or {}).get('StringValue')
