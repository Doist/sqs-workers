import datetime
from typing import Protocol

now = datetime.datetime.utcnow


class ShutdownPolicy(Protocol):
    def update_state(self, batch_processing_result) -> None:
        ...

    def need_shutdown(self) -> bool:
        ...


class NeverShutdown:
    """Never shutdown the worker"""

    def update_state(self, batch_processing_result) -> None:
        pass

    def need_shutdown(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "NeverShutdown()"


class IdleShutdown:
    """Shutdown worker if it's idle for certain time (set in seconds)"""

    def __init__(self, idle_seconds: int) -> None:
        self.idle_seconds = idle_seconds
        self._idle_delta = datetime.timedelta(seconds=idle_seconds)
        self._is_idle = False
        self._last_seen = now()

    def update_state(self, batch_processing_result) -> None:
        """Update internal state of the shutdown policy"""
        if batch_processing_result.total_count() == 0:
            self._is_idle = True
        else:
            self._is_idle = False
            self._last_seen = now()

    def need_shutdown(self) -> bool:
        if not self._is_idle:
            return False
        return now() - self._last_seen >= self._idle_delta

    def __repr__(self) -> str:
        return f"IdleShutdown({self.idle_seconds})"


class MaxTasksShutdown:
    """
    Shutdown worker if it executed more than max_tasks in total (both
    successfully or with error)
    """

    def __init__(self, max_tasks: int) -> None:
        self.max_tasks = max_tasks
        self._tasks = 0

    def update_state(self, batch_processing_result) -> None:
        self._tasks += batch_processing_result.total_count()

    def need_shutdown(self) -> bool:
        return self._tasks >= self.max_tasks

    def __repr__(self) -> str:
        return f"MaxTasksShutdown({self.max_tasks})"


class OrShutdown:
    """Return True if any of conditions of policies is met"""

    def __init__(self, *policies) -> None:
        self.policies = policies

    def update_state(self, batch_processing_result) -> None:
        for p in self.policies:
            p.update_state(batch_processing_result)

    def need_shutdown(self) -> bool:
        return any(p.need_shutdown() for p in self.policies)

    def __repr__(self) -> str:
        repr_policies = ", ".join([repr(p) for p in self.policies])
        return f"OrShutdown({repr_policies})"


class AndShutdown:
    """Return True if all of conditions of policies are met"""

    def __init__(self, *policies) -> None:
        self.policies = policies

    def update_state(self, batch_processing_result) -> None:
        for p in self.policies:
            p.update_state(batch_processing_result)

    def need_shutdown(self) -> bool:
        return all(p.need_shutdown() for p in self.policies)

    def __repr__(self) -> str:
        repr_policies = ", ".join([repr(p) for p in self.policies])
        return f"AndShutdown({repr_policies})"


NEVER_SHUTDOWN = NeverShutdown()
ASAP_SHUTDOWN = IdleShutdown(0)
