import json
import logging
import multiprocessing
from itertools import groupby

import boto3

from sqs_workers import codecs, processors, context
from sqs_workers.backoff_policies import DEFAULT_BACKOFF
from sqs_workers.codecs import DEFAULT_CONTENT_TYPE
from sqs_workers.processor_mgr import ProcessorManager, ProcessorManagerProxy
from sqs_workers.shutdown_policies import NEVER_SHUTDOWN, NeverShutdown

logger = logging.getLogger(__name__)

DEFAULT_MESSAGE_GROUP_ID = 'default'


class BatchProcessingResult(object):
    succeeded = None
    failed = None

    def __init__(self, queue_name, succeeded=None, failed=None):
        self.queue_name = queue_name
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
        return '<BatchProcessingResult/%s/%s/%s>' % (
            self.queue_name, self.succeeded_count(), self.failed_count())


class SQSEnv(ProcessorManagerProxy):
    def __init__(self,
                 session=boto3,
                 queue_prefix='',
                 backoff_policy=DEFAULT_BACKOFF,
                 processor_maker=processors.Processor,
                 batch_processor_maker=processors.BatchProcessor,
                 fallback_processor_maker=processors.FallbackProcessor,
                 context_maker=context.SQSContext):
        """
        Initialize SQS environment with boto3 session
        """
        self.session = session
        self.sqs_client = session.client('sqs')
        self.sqs_resource = session.resource('sqs')
        self.queue_prefix = queue_prefix
        self.context = context_maker()
        self.processors = ProcessorManager(
            self, backoff_policy, processor_maker, batch_processor_maker,
            fallback_processor_maker)

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

    def add_job(self,
                queue_name,
                job_name,
                _content_type=DEFAULT_CONTENT_TYPE,
                _delay_seconds=None,
                _deduplication_id=None,
                _group_id=None,
                **job_kwargs):
        """
        Add job to the queue. The body of the job will be converted to the text
        with one of the codes (by default it's "pickle")
        """
        codec = codecs.get_codec(_content_type)
        message_body = codec.serialize(job_kwargs)
        job_context = codec.serialize(self.context.to_dict())
        return self.add_raw_job(queue_name, job_name, message_body,
                                job_context, _content_type, _delay_seconds,
                                _deduplication_id, _group_id)

    def add_raw_job(self, queue_name, job_name, message_body, job_context,
                    content_type, delay_seconds, deduplication_id, group_id):
        """
        Low-level function to put message to the queue
        """
        # if queue name ends with .fifo, then according to the AWS specs,
        # it's a FIFO queue, and requires group_id.
        # Otherwise group_id can be set to None
        if group_id is None and queue_name.endswith('.fifo'):
            group_id = DEFAULT_MESSAGE_GROUP_ID

        queue = self.get_queue(queue_name)
        kwargs = {
            'MessageBody': message_body,
            'MessageAttributes': {
                'ContentType': {
                    'StringValue': content_type,
                    'DataType': 'String',
                },
                'JobContext': {
                    'StringValue': job_context,
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
        if deduplication_id is not None:
            kwargs['MessageDeduplicationId'] = str(deduplication_id)
        if group_id is not None:
            kwargs['MessageGroupId'] = str(group_id)
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
        result = BatchProcessingResult(queue_name)

        for job_name, job_messages in group_messages(queue_name, messages):
            processor = self.processors.get(queue_name, job_name)
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

    def get_sqs_queue_name(self, queue_name):
        """
        Take "high-level" (user-visible) queue name and return SQS
        ("low level") name by simply prefixing it. Used to create namespaces
        for different environments (development, staging, production, etc)
        """
        return '{}{}'.format(self.queue_prefix, queue_name)

    def redrive_policy(self, dead_letter_queue_name, max_receive_count):
        return RedrivePolicy(self, dead_letter_queue_name, max_receive_count)


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


def group_messages(queue_name, messages):
    """
    Group SQSMessage instances by JobName attribute. If queue name ends with
    .fifo (it's a FIFO queue), we don't reorder messages while grouping,
    otherwise messages can get out of the grouper reorderd
    """
    if not queue_name.endswith('.fifo'):
        messages = sorted(messages, key=get_job_name)
    for job_name, job_messages in groupby(messages, key=get_job_name):
        yield job_name, list(job_messages)


def get_job_name(message):
    attrs = message.message_attributes or {}
    return (attrs.get('JobName') or {}).get('StringValue')
