import logging
import multiprocessing

import boto3

from sqs_workers import DEFAULT_BACKOFF, context, processors
from sqs_workers.core import RedrivePolicy
from sqs_workers.processor_mgr import ProcessorManager
from sqs_workers.queue import GenericQueue
from sqs_workers.shutdown_policies import NeverShutdown

logger = logging.getLogger(__name__)


class SQSEnv(object):
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
        self.queues = {}  # type: dict[str, GenericQueue]

    def queue(self, queue_name):
        if queue_name not in self.queues:
            self.queues[queue_name] = GenericQueue(self, queue_name)
        return self.queues[queue_name]

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
            queue = self.queue(queue_name)
            p = multiprocessing.Process(
                target=queue.process_queue,
                kwargs={"shutdown_policy": shutdown_policy_maker()},
            )
            p.start()
            processes.append(p)
        for p in processes:
            p.join()

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
