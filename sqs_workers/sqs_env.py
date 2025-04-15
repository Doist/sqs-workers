from __future__ import annotations

import logging
import multiprocessing
import warnings
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    TypeVar,
    Union,
    overload,
)

import boto3
from botocore.config import Config
from typing_extensions import ParamSpec

from sqs_workers import DEFAULT_BACKOFF, RawQueue, codecs, context, processors
from sqs_workers.batching import BatchingConfiguration, NoBatching
from sqs_workers.core import RedrivePolicy
from sqs_workers.processors import DEFAULT_CONTEXT_VAR
from sqs_workers.queue import GenericQueue, JobQueue
from sqs_workers.shutdown_policies import NeverShutdown, ShutdownPolicy

if TYPE_CHECKING:
    from sqs_workers.async_task import AsyncTask
    from sqs_workers.backoff_policies import BackoffPolicy


logger = logging.getLogger(__name__)

POLICY_NO_BATCHING = NoBatching()


AnyQueue = Union[GenericQueue, JobQueue, RawQueue]
AnyQueueT = TypeVar("AnyQueueT", GenericQueue, JobQueue, RawQueue)

P = ParamSpec("P")
R = TypeVar("R")


@dataclass
class SQSEnv:
    session: Any = boto3
    queue_prefix: str = ""
    codec: str = codecs.DEFAULT_CONTENT_TYPE

    # retry settings for internal boto
    retry_max_attempts: int = 3
    retry_mode: str = "standard"

    # queue-specific settings
    backoff_policy: Any = DEFAULT_BACKOFF

    # jobqueue-specific settings
    processor_maker: Any = processors.Processor
    context_maker: Any = context.SQSContext

    # internal attributes
    context: Any = None
    sqs_client: Any = None
    sqs_resource: Any = None
    queues: dict[str, AnyQueue] = field(default_factory=dict)

    def __post_init__(self):
        self.context = self.context_maker()
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html
        retry_dict = {"max_attempts": self.retry_max_attempts, "mode": self.retry_mode}
        retry_config = Config(retries=retry_dict)
        if not self.sqs_client:
            self.sqs_client = self.session.client("sqs", config=retry_config)
        if not self.sqs_resource:
            self.sqs_resource = self.session.resource("sqs", config=retry_config)

    @overload
    def queue(
        self,
        queue_name: str,
        queue_maker: type[JobQueue] | Callable[..., JobQueue] = JobQueue,
        batching_policy: BatchingConfiguration = POLICY_NO_BATCHING,
        backoff_policy: BackoffPolicy | None = None,
    ) -> JobQueue: ...

    @overload
    def queue(
        self,
        queue_name: str,
        queue_maker: type[AnyQueueT] | Callable[..., AnyQueueT],
        batching_policy: BatchingConfiguration = POLICY_NO_BATCHING,
        backoff_policy: BackoffPolicy | None = None,
    ) -> AnyQueueT: ...

    def queue(
        self,
        queue_name: str,
        queue_maker: type[AnyQueue] | Callable[..., AnyQueue] = JobQueue,
        batching_policy: BatchingConfiguration = POLICY_NO_BATCHING,
        backoff_policy: BackoffPolicy | None = None,
    ) -> AnyQueue:
        """Get a queue object, initializing it with queue_maker if necessary."""
        if queue_name not in self.queues:
            backoff_policy = backoff_policy or self.backoff_policy
            self.queues[queue_name] = queue_maker(
                env=self,
                name=queue_name,
                batching_policy=batching_policy,
                backoff_policy=backoff_policy,
            )
        return self.queues[queue_name]

    def processor(
        self,
        queue_name: str,
        job_name: str,
        pass_context: bool = False,
        context_var=DEFAULT_CONTEXT_VAR,
    ) -> Callable[[Callable[P, Any]], AsyncTask[P]]:
        """Decorator to attach processor to all jobs "job_name" of the queue "queue_name"."""
        q: JobQueue = self.queue(queue_name, queue_maker=JobQueue)
        return q.processor(
            job_name=job_name, pass_context=pass_context, context_var=context_var
        )

    def raw_processor(self, queue_name: str):
        """Decorator to attach raw processor to all jobs of the queue "queue_name"."""
        q: RawQueue = self.queue(queue_name, queue_maker=RawQueue)
        return q.raw_processor()

    def add_job(
        self,
        queue_name,
        job_name,
        _content_type=None,
        _delay_seconds=None,
        _deduplication_id=None,
        _group_id=None,
        **job_kwargs,
    ):
        """Add job to the queue."""
        warnings.warn(
            "sqs.add_job() is deprecated. Use sqs.queue(...).add_job() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        if not _content_type:
            _content_type = self.codec
        q = self.queue(queue_name, queue_maker=JobQueue)
        return q.add_job(
            job_name,
            _content_type=_content_type,
            _delay_seconds=_delay_seconds,
            _deduplication_id=_deduplication_id,
            _group_id=_group_id,
            **job_kwargs,
        )

    def process_queues(
        self,
        queue_names=None,
        shutdown_policy_maker: Callable[[], ShutdownPolicy] = NeverShutdown,
    ):
        """
        Use multiprocessing to process multiple queues at once. If queue names
        are not set, process all known queues.

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
        resp = self.sqs_client.list_queues(QueueNamePrefix=self.queue_prefix)
        if "QueueUrls" not in resp:
            return []
        urls = resp["QueueUrls"]
        ret = []
        for url in urls:
            sqs_name = url.rsplit("/", 1)[-1]
            queue_prefix_len = len(self.queue_prefix)
            ret.append(sqs_name[queue_prefix_len:])
        return ret

    def get_sqs_queue_name(self, queue_name: str) -> str:
        """
        Take "high-level" (user-visible) queue name and return SQS
        ("low level") name by simply prefixing it. Used to create namespaces
        for different environments (development, staging, production, etc).
        """
        return f"{self.queue_prefix}{queue_name}"

    def redrive_policy(self, dead_letter_queue_name, max_receive_count):
        return RedrivePolicy(self, dead_letter_queue_name, max_receive_count)
