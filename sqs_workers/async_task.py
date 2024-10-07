from __future__ import annotations

from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generator,
    Generic,
    NoReturn,
    Optional,
    TypedDict,
)

from typing_extensions import NotRequired, ParamSpec

from sqs_workers.utils import bind_arguments

if TYPE_CHECKING:
    from sqs_workers.queue import JobQueue


P = ParamSpec("P")


class AsyncTaskDelayArgs(TypedDict):
    """
    Extra arguments accepted by `AsyncTask.delay`. This mapping can be used with `Unpack` to
    improve typing of task function when `delay` is used.

    `def task(..., **_delay_args: Unpack[AsyncTaskDelayArgs]) -> None: ...`
    """

    _content_type: NotRequired[str]
    _delay_seconds: NotRequired[float]
    _deduplication_id: NotRequired[str]
    _group_id: NotRequired[str]


class AsyncTask(Generic[P]):
    def __init__(
        self, queue: JobQueue, job_name: str, processor: Callable[P, Any]
    ) -> None:
        self.queue = queue
        self.job_name = job_name
        self.processor = processor
        self.__doc__ = processor.__doc__

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> NoReturn:
        raise RuntimeError(
            f"Async task {self.queue.name}.{self.job_name} called synchronously "
            "(probably, by mistake). Use either AsyncTask.run(...) "
            "to run the task synchronously or AsyncTask.delay(...) "
            "to add it to the queue"
        )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.queue.name}.{self.job_name}>"

    def run(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        """Run the task synchronously."""
        if self.queue.batching_policy.batching_enabled:
            if len(args) > 0:
                raise TypeError("Must use keyword arguments only for batch read queues")
            kwargs = bind_arguments(self.processor, [[kwargs]], {})
            return self.processor(**kwargs)
        else:
            kwargs = bind_arguments(self.processor, args, kwargs)
            return self.processor(**kwargs)

    @contextmanager
    def batch(self) -> Generator[None, None, None]:
        """Context manager to add jobs in batch."""
        with self.queue.add_batch():
            yield

    def delay(self, *args: P.args, **kwargs: P.kwargs) -> Optional[str]:
        """Run the task asynchronously."""
        _content_type = kwargs.pop("_content_type", self.queue.env.codec)  # type: ignore
        _delay_seconds = kwargs.pop("_delay_seconds", None)  # type: ignore
        _deduplication_id = kwargs.pop("_deduplication_id", None)  # type: ignore
        _group_id = kwargs.pop("_group_id", None)  # type: ignore

        if self.queue.batching_policy.batching_enabled:
            if len(args) > 0:
                raise TypeError("Must use keyword arguments only for batch read queues")
        else:
            kwargs = bind_arguments(self.processor, args, kwargs)

        return self.queue.add_job(
            self.job_name,
            _content_type=_content_type,  # type: ignore
            _delay_seconds=_delay_seconds,  # type: ignore
            _deduplication_id=_deduplication_id,
            _group_id=_group_id,  # type: ignore
            **kwargs,
        )

    def bake(self, *args: P.args, **kwargs: P.kwargs) -> BakedAsyncTask:
        """
        Create a baked version of the async task, which contains the reference
        to a queue and task, as well as all arguments which needs to be passed
        to it.
        """
        return BakedAsyncTask(self, args, kwargs)


class BakedAsyncTask:
    def __init__(self, async_task, args, kwargs):
        self.async_task = async_task
        self.args = args
        self.kwargs = kwargs

    def __call__(self) -> None:
        self.async_task(*self.args, **self.kwargs)

    def delay(self) -> None:
        self.async_task.delay(*self.args, **self.kwargs)

    def __repr__(self) -> str:
        return "BakedAsyncTask(%r, ...)" % self.async_task
