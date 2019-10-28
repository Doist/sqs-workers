import logging
import multiprocessing
import warnings
from typing import Type

import attr
import boto3

from sqs_workers import DEFAULT_BACKOFF, RawQueue, codecs, context, processors
from sqs_workers.core import RedrivePolicy
from sqs_workers.processors import DEFAULT_CONTEXT_VAR
from sqs_workers.queue import GenericQueue, JobQueue
from sqs_workers.shutdown_policies import NeverShutdown

logger = logging.getLogger(__name__)


@attr.s
class SQSEnv(object):

    session = attr.ib(default=boto3)
    queue_prefix = attr.ib(default="")

    # queue-specific settings
    backoff_policy = attr.ib(default=DEFAULT_BACKOFF)

    # jobqueue-specific settings
    processor_maker = attr.ib(default=processors.Processor)
    context_maker = attr.ib(default=context.SQSContext)

    # internal attributes
    context = attr.ib(default=None)
    sqs_client = attr.ib(default=None)
    sqs_resource = attr.ib(default=None)
    queues = attr.ib(init=False, factory=dict)  # type: dict[str, JobQueue]

    def __attrs_post_init__(self):
        self.context = self.context_maker()
        self.sqs_client = self.session.client("sqs")
        self.sqs_resource = self.session.resource("sqs")

    def queue(self, queue_name, queue_maker=JobQueue, backoff_policy=None):
        # type: (str, Type[GenericQueue]) -> GenericQueue
        """
        Get a queue object, initializing it with queue_maker if necessary.
        """
        if queue_name not in self.queues:
            backoff_policy = backoff_policy or self.backoff_policy
            self.queues[queue_name] = queue_maker(
                env=self, name=queue_name, backoff_policy=backoff_policy
            )
        return self.queues[queue_name]

    def processor(
        self, queue_name, job_name, pass_context=False, context_var=DEFAULT_CONTEXT_VAR
    ):
        """
        Decorator to attach processor to all jobs "job_name" of the queue "queue_name".
        """
        q = self.queue(queue_name, queue_maker=JobQueue)  # type: JobQueue
        return q.processor(
            job_name=job_name, pass_context=pass_context, context_var=context_var
        )

    def raw_processor(self, queue_name):
        """
        Decorator to attach raw processor to all jobs of the queue "queue_name".
        """
        q = self.queue(queue_name, queue_maker=RawQueue)  # type: RawQueue
        return q.raw_processor()

    def add_job(
        self,
        queue_name,
        job_name,
        _content_type=codecs.DEFAULT_CONTENT_TYPE,
        _delay_seconds=None,
        _deduplication_id=None,
        _group_id=None,
        **job_kwargs
    ):
        """
        Add job to the queue.
        """
        warnings.warn(
            "sqs.add_job() is deprecated. Use sqs.queue(...).add_job() instead",
            DeprecationWarning,
        )
        q = self.queue(queue_name, queue_maker=JobQueue)  # type: JobQueue
        return q.add_job(
            job_name,
            _content_type=_content_type,
            _delay_seconds=_delay_seconds,
            _deduplication_id=_deduplication_id,
            _group_id=_group_id,
            **job_kwargs
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
        return "{}{}".format(self.queue_prefix, queue_name)

    def redrive_policy(self, dead_letter_queue_name, max_receive_count):
        return RedrivePolicy(self, dead_letter_queue_name, max_receive_count)
