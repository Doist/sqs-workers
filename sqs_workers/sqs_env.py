import logging
import multiprocessing

import boto3

from sqs_workers import DEFAULT_BACKOFF, codecs, context, processors
from sqs_workers.codecs import DEFAULT_CONTENT_TYPE
from sqs_workers.core import BatchProcessingResult, RedrivePolicy, get_job_name
from sqs_workers.processor_mgr import ProcessorManager, ProcessorManagerProxy
from sqs_workers.shutdown_policies import NEVER_SHUTDOWN, NeverShutdown

logger = logging.getLogger(__name__)
DEFAULT_MESSAGE_GROUP_ID = "default"


class SQSEnv(ProcessorManagerProxy):
    def __init__(
        self,
        session=boto3,
        queue_prefix="",
        backoff_policy=DEFAULT_BACKOFF,
        processor_maker=processors.Processor,
        fallback_processor_maker=processors.FallbackProcessor,
        context_maker=context.SQSContext,
    ):
        """
        Initialize SQS environment with boto3 session
        """
        self.session = session
        self.sqs_client = session.client("sqs")
        self.sqs_resource = session.resource("sqs")
        self.queue_prefix = queue_prefix
        self.context = context_maker()
        self.processors = ProcessorManager(
            self, backoff_policy, processor_maker, fallback_processor_maker
        )
        self.queues = {}  # type: dict[str, SQSEnvQueue]

    def queue(self, queue_name):
        if queue_name not in self.queues:
            self.queues[queue_name] = SQSEnvQueue(self, queue_name)
        return self.queues[queue_name]

    def create_standard_queue(
        self,
        queue_name,
        message_retention_period=None,
        visibility_timeout=None,
        redrive_policy=None,
    ):
        """
        Create a new standard queue
        """
        return self.queue(queue_name).create_standard_queue(
            message_retention_period, visibility_timeout, redrive_policy
        )

    def create_fifo_queue(
        self,
        queue_name,
        content_based_deduplication=False,
        message_retention_period=None,
        visibility_timeout=None,
        redrive_policy=None,
    ):
        """
        Create a new FIFO queue. Note that queue name has to end with ".fifo"

        - "content_based_deduplication" turns on automatic content-based
          deduplication of messages in the queue

        - redrive_policy can be None or an object, generated with
          redrive_policy() method of SQS. In the latter case if defines the
          way failed messages are processed.
        """
        return self.queue(queue_name).create_fifo_queue(
            content_based_deduplication,
            message_retention_period,
            visibility_timeout,
            redrive_policy,
        )

    def purge_queue(self, queue_name):
        """
        Remove all messages from the queue
        """
        return self.queue(queue_name).purge_queue()

    def delete_queue(self, queue_name):
        """
        Delete the queue
        """
        return self.queue(queue_name).delete_queue()

    def add_job(
        self,
        queue_name,
        job_name,
        _content_type=DEFAULT_CONTENT_TYPE,
        _delay_seconds=None,
        _deduplication_id=None,
        _group_id=None,
        **job_kwargs
    ):
        """
        Add job to the queue. The body of the job will be converted to the text
        with one of the codes (by default it's "pickle")
        """
        return self.queue(queue_name).add_job(
            job_name,
            _content_type,
            _delay_seconds,
            _deduplication_id,
            _group_id,
            **job_kwargs
        )

    def add_raw_job(
        self,
        queue_name,
        job_name,
        message_body,
        job_context,
        content_type,
        delay_seconds,
        deduplication_id,
        group_id,
    ):
        """
        Low-level function to put message to the queue
        """
        return self.queue(queue_name).add_raw_job(
            job_name,
            message_body,
            job_context,
            content_type,
            delay_seconds,
            deduplication_id,
            group_id,
        )

    def process_queues(self, queue_names=None, shutdown_policy_maker=NeverShutdown):
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
                    "queue_name": queue_name,
                    "shutdown_policy": shutdown_policy_maker(),
                },
            )
            p.start()
            processes.append(p)
        for p in processes:
            p.join()

    def drain_queue(self, queue_name, wait_seconds=0):
        """
        Delete all messages from the queue without calling purge()
        """
        return self.queue(queue_name).drain_queue(wait_seconds)

    def process_queue(self, queue_name, shutdown_policy=NEVER_SHUTDOWN, wait_second=10):
        """
        Run worker to process one queue in the infinite loop
        """
        return self.queue(queue_name).process_queue(shutdown_policy, wait_second)

    def process_batch(self, queue_name, wait_seconds=0):
        # type: (str, int) -> BatchProcessingResult
        """
        Process a batch of messages from the queue (10 messages at most), return
        the number of successfully processed messages, and exit
        """
        return self.queue(queue_name).process_batch(wait_seconds)

    def get_raw_messages(self, queue_name, wait_seconds):
        """
        Return raw messages from the queue, addressed by its name
        """
        return self.queue(queue_name).get_raw_messages(wait_seconds)

    def get_queue(self, queue_name):
        """
        Helper function to return queue object by name
        """
        return self.queue(queue_name).get_queue()

    def get_all_known_queues(self):
        resp = self.sqs_client.list_queues(**{"QueueNamePrefix": self.queue_prefix})
        if "QueueUrls" not in resp:
            return []
        urls = resp["QueueUrls"]
        ret = []
        for url in urls:
            sqs_name = url.rsplit("/", 1)[-1]
            queue_prefix_len = len(self.queue_prefix)
            ret.append(sqs_name[queue_prefix_len:])
        return ret

    def get_sqs_queue_name(self, queue_name):
        """
        Take "high-level" (user-visible) queue name and return SQS
        ("low level") name by simply prefixing it. Used to create namespaces
        for different environments (development, staging, production, etc)
        """
        return self.queue(queue_name).get_sqs_queue_name()

    def redrive_policy(self, dead_letter_queue_name, max_receive_count):
        return RedrivePolicy(self, dead_letter_queue_name, max_receive_count)


class SQSEnvQueue(object):
    def __init__(self, env, name):
        # type: (SQSEnv, str) -> None
        self.env = env
        self.name = name
        self._queue = None

    def create_standard_queue(
        self,
        message_retention_period=None,
        visibility_timeout=None,
        redrive_policy=None,
    ):
        """
        Create a new standard queue
        """
        attrs = {}
        kwargs = {"QueueName": self.get_sqs_queue_name(), "Attributes": attrs}
        if message_retention_period is not None:
            attrs["MessageRetentionPeriod"] = str(message_retention_period)
        if visibility_timeout is not None:
            attrs["VisibilityTimeout"] = str(visibility_timeout)
        if redrive_policy is not None:
            attrs["RedrivePolicy"] = redrive_policy.__json__()
        ret = self.env.sqs_client.create_queue(**kwargs)
        return ret["QueueUrl"]

    def create_fifo_queue(
        self,
        content_based_deduplication=False,
        message_retention_period=None,
        visibility_timeout=None,
        redrive_policy=None,
    ):
        """
        Create a new FIFO queue. Note that queue name has to end with ".fifo"

        - "content_based_deduplication" turns on automatic content-based
          deduplication of messages in the queue

        - redrive_policy can be None or an object, generated with
          redrive_policy() method of SQS. In the latter case if defines the
          way failed messages are processed.
        """
        attrs = {"FifoQueue": "true"}
        kwargs = {"QueueName": self.get_sqs_queue_name(), "Attributes": attrs}
        if content_based_deduplication:
            attrs["ContentBasedDeduplication"] = "true"
        if message_retention_period is not None:
            attrs["MessageRetentionPeriod"] = str(message_retention_period)
        if visibility_timeout is not None:
            attrs["VisibilityTimeout"] = str(visibility_timeout)
        if redrive_policy is not None:
            attrs["RedrivePolicy"] = redrive_policy.__json__()
        ret = self.env.sqs_client.create_queue(**kwargs)
        return ret["QueueUrl"]

    def purge_queue(self):
        """
        Remove all messages from the queue
        """
        self.get_queue().purge()

    def delete_queue(self):
        """
        Delete the queue
        """
        self.get_queue().delete()

    def add_job(
        self,
        job_name,
        _content_type=DEFAULT_CONTENT_TYPE,
        _delay_seconds=None,
        _deduplication_id=None,
        _group_id=None,
        **job_kwargs
    ):
        """
        Add job to the queue. The body of the job will be converted to the text
        with one of the codes (by default it's "pickle")
        """
        codec = codecs.get_codec(_content_type)
        message_body = codec.serialize(job_kwargs)
        job_context = codec.serialize(self.env.context.to_dict())
        return self.add_raw_job(
            job_name,
            message_body,
            job_context,
            _content_type,
            _delay_seconds,
            _deduplication_id,
            _group_id,
        )

    def add_raw_job(
        self,
        job_name,
        message_body,
        job_context,
        content_type,
        delay_seconds,
        deduplication_id,
        group_id,
    ):
        """
        Low-level function to put message to the queue
        """
        # if queue name ends with .fifo, then according to the AWS specs,
        # it's a FIFO queue, and requires group_id.
        # Otherwise group_id can be set to None
        if group_id is None and self.name.endswith(".fifo"):
            group_id = DEFAULT_MESSAGE_GROUP_ID

        queue = self.get_queue()
        kwargs = {
            "MessageBody": message_body,
            "MessageAttributes": {
                "ContentType": {"StringValue": content_type, "DataType": "String"},
                "JobContext": {"StringValue": job_context, "DataType": "String"},
                "JobName": {"StringValue": job_name, "DataType": "String"},
            },
        }
        if delay_seconds is not None:
            kwargs["DelaySeconds"] = int(delay_seconds)
        if deduplication_id is not None:
            kwargs["MessageDeduplicationId"] = str(deduplication_id)
        if group_id is not None:
            kwargs["MessageGroupId"] = str(group_id)
        ret = queue.send_message(**kwargs)
        return ret["MessageId"]

    def drain_queue(self, wait_seconds=0):
        """
        Delete all messages from the queue without calling purge()
        """
        queue = self.get_queue()
        deleted_count = 0
        while True:
            messages = self.get_raw_messages(wait_seconds)
            if not messages:
                break
            entries = [
                {"Id": msg.message_id, "ReceiptHandle": msg.receipt_handle}
                for msg in messages
            ]
            queue.delete_messages(Entries=entries)
            deleted_count += len(messages)
        return deleted_count

    def process_queue(self, shutdown_policy=NEVER_SHUTDOWN, wait_second=10):
        """
        Run worker to process one queue in the infinite loop
        """
        logger.debug(
            "Start processing queue {}".format(self.name),
            extra={
                "queue_name": self.name,
                "wait_seconds": wait_second,
                "shutdown_policy": repr(shutdown_policy),
            },
        )
        while True:
            result = self.process_batch(wait_seconds=wait_second)
            shutdown_policy.update_state(result)
            if shutdown_policy.need_shutdown():
                logger.debug(
                    "Stop processing queue {}".format(self.name),
                    extra={
                        "queue_name": self.name,
                        "wait_seconds": wait_second,
                        "shutdown_policy": repr(shutdown_policy),
                    },
                )
                break

    def process_batch(self, wait_seconds=0):
        # type: (int) -> BatchProcessingResult
        """
        Process a batch of messages from the queue (10 messages at most), return
        the number of successfully processed messages, and exit
        """
        queue = self.get_queue()
        messages = self.get_raw_messages(wait_seconds)
        result = BatchProcessingResult(self.name)

        for message in messages:
            job_name = get_job_name(message)
            processor = self.env.processors.get(self.name, job_name)
            success = processor.process_message(message)
            result.update_with_message(message, success)
            if success:
                entry = {
                    "Id": message.message_id,
                    "ReceiptHandle": message.receipt_handle,
                }
                queue.delete_messages(Entries=[entry])
            else:
                timeout = processor.backoff_policy.get_visibility_timeout(message)
                entry = {
                    "Id": message.message_id,
                    "ReceiptHandle": message.receipt_handle,
                    "VisibilityTimeout": timeout,
                }
                queue.change_message_visibility_batch(Entries=[entry])
        return result

    def get_raw_messages(self, wait_seconds):
        """
        Return raw messages from the queue, addressed by its name
        """
        kwargs = {
            "WaitTimeSeconds": wait_seconds,
            "MaxNumberOfMessages": 10,
            "MessageAttributeNames": ["All"],
            "AttributeNames": ["All"],
        }
        queue = self.get_queue()
        return queue.receive_messages(**kwargs)

    def get_queue(self):
        """
        Helper function to return queue object by name
        """
        if self._queue is None:
            self._queue = self.env.sqs_resource.get_queue_by_name(
                QueueName=self.get_sqs_queue_name()
            )
        return self._queue

    def get_sqs_queue_name(self):
        """
        Take "high-level" (user-visible) queue name and return SQS
        ("low level") name by simply prefixing it. Used to create namespaces
        for different environments (development, staging, production, etc)
        """
        return "{}{}".format(self.env.queue_prefix, self.name)
