import datetime
import logging
import multiprocessing
import time
import uuid
from collections import defaultdict
from itertools import groupby
from queue import Empty, Queue

from sqs_workers import DEFAULT_BACKOFF, codecs, processors
from sqs_workers.core import (DEFAULT_CONTENT_TYPE, AsyncBatchTask, AsyncTask,
                              BatchProcessingResult, RedrivePolicy,
                              get_job_name)
from sqs_workers.shutdown_policies import NEVER_SHUTDOWN, NeverShutdown

logger = logging.getLogger(__name__)


class MemoryEnv(object):
    """
    In-memory pseudo SQS implementation, for faster and more predictable
    processing in tests.

    Implements the SQSEnv interface, but lacks some features of it, and some
    other features implemented ineffectively.

    - Redrive policy doesn't work
    - There is no differences between standard and FIFO queues
    - FIFO queues don't support content-based deduplication
    - Delayed tasks executed ineffectively: the task is gotten from the queue,
      and if the time hasn't come yet, the task is put back.
    - API can return slightly different results
    """

    # TODO: maybe it can be implemented more effectively with sched

    def __init__(self,
                 backoff_policy=DEFAULT_BACKOFF,
                 fallback_processor_maker=processors.FallbackProcessor):
        """
        Initialize pseudo SQS environment
        """
        self.backoff_policy = backoff_policy
        self.processors = defaultdict(lambda: {})
        self.fallback_processor_maker = fallback_processor_maker
        self.queues = {}  # type: dict[str, Queue]

    def create_standard_queue(self, queue_name, **kwargs):
        """
        Create a new standard queue
        """
        self.queues[queue_name] = Queue()
        return 'memory://%s' % queue_name

    def create_fifo_queue(self, queue_name, **kwargs):
        """
        Create a new FIFO queue. Implementation is the same as for the standard
        queue
        """
        self.queues[queue_name] = Queue()
        return 'memory://%s' % queue_name

    def purge_queue(self, queue_name):
        """
        Remove all messages from the queue
        """
        queue = self.queues[queue_name]
        while True:
            try:
                queue.get_nowait()
            except Empty:
                return

    def delete_queue(self, queue_name):
        """
        Delete the queue
        """
        self.queues.pop(queue_name)

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
        self.processors[queue_name][job_name] = processors.Processor(self,
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
        self.processors[queue_name][job_name] = processors.BatchProcessor(self,
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

    def copy_processors(self, src_queue, dst_queue):
        """
        Copy processors from src_queue to dst_queue (both queues identified
        by their names). Can be helpful to process dead-letter queue with
        processors from the main queue.

        Usage example.

        sqs = SQSEnv()
        ...
        sqs.copy_processors('foo', 'foo_dead')
        sqs.process_queue("foo_dead", shutdown_policy=IdleShutdown(10))

        Here the queue "foo_dead" will be processed with processors from the
        queue "foo".
        """
        for job_name, processor in self.processors[src_queue].items():
            self.processors[dst_queue][job_name] = processor.copy(
                queue_name=dst_queue)

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
            delay_seconds_int = int(delay_seconds)
            execute_at = datetime.datetime.utcnow() + datetime.timedelta(
                seconds=delay_seconds)
            kwargs['DelaySeconds'] = delay_seconds_int
            kwargs['_execute_at'] = execute_at
        queue = self.queues[queue_name]
        queue.put(kwargs)
        return ''

    def process_queues(self,
                       queue_names=None,
                       shutdown_policy_maker=NeverShutdown):
        """
        Use multiprocessing to process multiple queues at once. If queue names
        are not set, process all known queues

        shutdown_policy_maker is an optional callable which doesn't accept any
        arguments and create a new shutdown policy for each queue.
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
        Delete all messages from the queue. An equivalent to purge()
        """
        self.purge_queue(queue_name)

    def process_queue(self,
                      queue_name,
                      shutdown_policy=NEVER_SHUTDOWN,
                      wait_second=10):
        """
        Run worker to process one queue in the infinite loop
        """
        while True:
            result = self.process_batch(queue_name, wait_seconds=wait_second)
            shutdown_policy.update_state(result)
            if shutdown_policy.need_shutdown():
                break

    def process_batch(self, queue_name, wait_seconds=0):
        # type: (str, int) -> BatchProcessingResult
        """
        Process a batch of messages from the queue (10 messages at most), return
        the number of successfully processed messages, and exit
        """
        queue = self.queues[queue_name]
        messages = self.get_raw_messages(queue_name, wait_seconds)
        result = BatchProcessingResult()
        for job_name, job_messages in self.group_messages(messages):
            processor = self.get_processor(queue_name, job_name)
            succeeded, failed = processor.process_batch(job_messages)
            result.update(succeeded, failed)
            if failed:
                for msg in failed:
                    queue.put(msg)
        return result

    def get_raw_messages(self, queue_name, wait_seconds=0):
        """
        Helper function to get at most 10 messages from the queue, waiting for
        wait_seconds at most before they get ready.
        """
        max_messages = 10
        sleep_interval = 0.1

        messages = []
        while True:
            messages += self._get_some_raw_messages(queue_name, max_messages)
            if len(messages) >= max_messages:
                break
            if wait_seconds <= 0:
                break
            wait_seconds -= sleep_interval
            time.sleep(sleep_interval)

        return messages

    def _get_some_raw_messages(self, queue_name, max_messages):
        """
        Helper function which returns at most max_messages from the
        queue. Used in an infinite loop inside `get_raw_messages`
        """
        queue = self.queues[queue_name]
        messages = []
        for i in range(max_messages):
            try:
                messages.append(RawMessage(queue.get_nowait()))
            except Empty:
                break

        now = datetime.datetime.utcnow()
        ready_messages = []
        for message in messages:
            execute_at = message.get('_execute_at', now)
            if execute_at > now:
                queue.put(message)
            else:
                ready_messages.append(message)

        return ready_messages

    def get_all_known_queues(self):
        return list(self.queues.keys())

    def get_processor(self, queue_name, job_name):
        """
        Helper function to return a processor for the queue
        """
        processor = self.processors[queue_name].get(job_name)
        if processor is None:
            processor = self.fallback_processor_maker(self, queue_name, job_name)
            self.processors[queue_name][job_name] = processor
        return processor

    def group_messages(self, messages):
        messages = sorted(messages, key=get_job_name)
        for job_name, job_messages in groupby(messages, key=get_job_name):
            yield job_name, list(job_messages)

    def get_sqs_queue_name(self, queue_name):
        return queue_name

    def redrive_policy(self, dead_letter_queue_name, max_receive_count):
        return RedrivePolicy(self, dead_letter_queue_name, max_receive_count)


class RawMessage(dict):
    """
    A mock class to mimic the AWS message
    """

    @property
    def message_attributes(self):
        return self['MessageAttributes']

    @property
    def body(self):
        return self['MessageBody']

    @property
    def message_id(self):
        return uuid.uuid4().hex
