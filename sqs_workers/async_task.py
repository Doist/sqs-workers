from contextlib import contextmanager
from typing import TYPE_CHECKING, Callable

from sqs_workers.utils import bind_arguments

if TYPE_CHECKING:
    from sqs_workers.queue import JobQueue


class AsyncTask(object):
    def __init__(self, queue: "JobQueue", job_name: str, processor: Callable):
        self.queue = queue
        self.job_name = job_name
        self.processor = processor
        self.__doc__ = processor.__doc__

    def __call__(self, *args, **kwargs):
        raise RuntimeError(
            "Async task {0.queue.name}.{0.job_name} called synchronously (probably, "
            "by mistake). Use either AsyncTask.run(...) to run the task synchronously "
            "or AsyncTask.delay(...) to add it to the queue".format(self)
        )

    def __repr__(self):
        return "<%s %s.%s>" % (self.__class__.__name__, self.queue.name, self.job_name)

    def run(self, *args, **kwargs):
        """
        Run the task synchronously.
        """
        if self.queue.batching_policy.batching_enabled:
            if len(args) > 0:
                raise TypeError("Must use keyword arguments only for batch read queues")
            kwargs = bind_arguments(self.processor, [[kwargs]], {})
            return self.processor(**kwargs)
        else:
            kwargs = bind_arguments(self.processor, args, kwargs)
            return self.processor(**kwargs)

    @contextmanager
    def batch(self):
        """
        Context manager to add jobs in batch.
        """
        with self.queue.add_batch():
            yield

    def delay(self, *args, **kwargs):
        """
        Run the task asynchronously.
        """
        _content_type = kwargs.pop("_content_type", self.queue.env.codec)
        _delay_seconds = kwargs.pop("_delay_seconds", None)
        _deduplication_id = kwargs.pop("_deduplication_id", None)
        _group_id = kwargs.pop("_group_id", None)

        if self.queue.batching_policy.batching_enabled:
            if len(args) > 0:
                raise TypeError("Must use keyword arguments only for batch read queues")
        else:
            kwargs = bind_arguments(self.processor, args, kwargs)

        return self.queue.add_job(
            self.job_name,
            _content_type=_content_type,
            _delay_seconds=_delay_seconds,
            _deduplication_id=_deduplication_id,
            _group_id=_group_id,
            **kwargs
        )

    def bake(self, *args, **kwargs):
        """
        Create a baked version of the async task, which contains the reference
        to a queue and task, as well as all arguments which needs to be passed
        to it.
        """
        return BakedAsyncTask(self, args, kwargs)


class BakedAsyncTask(object):
    def __init__(self, async_task, args, kwargs):
        self.async_task = async_task
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        self.async_task(*self.args, **self.kwargs)

    def delay(self):
        self.async_task.delay(*self.args, **self.kwargs)

    def __repr__(self):
        return "BakedAsyncTask(%r, ...)" % self.async_task
