import time

from sqs_workers.core import BatchProcessingResult
from sqs_workers.shutdown_policies import (
    AndShutdown,
    IdleShutdown,
    MaxTasksShutdown,
    OrShutdown,
)


def test_idle_shutdown():
    s = IdleShutdown(idle_seconds=1)

    # empty result
    res = BatchProcessingResult("foo")
    s.update_state(res)

    # it's idle, but less than a second
    assert not s.need_shutdown()

    # it's idle for more than a second
    time.sleep(1.5)
    assert s.need_shutdown()


def test_max_tasks_shutdown():
    s = MaxTasksShutdown(max_tasks=2)
    assert not s.need_shutdown()
    # one succeeded task contributes, but not enough yet
    s.update_state(BatchProcessingResult("foo", [object()], []))
    assert not s.need_shutdown()
    # second failed task contributes just enough to flip the switch
    s.update_state(BatchProcessingResult("foo", [], [object()]))
    assert s.need_shutdown()


def test_or_policy():
    s1 = MaxTasksShutdown(max_tasks=1)
    s2 = MaxTasksShutdown(max_tasks=2)
    s = OrShutdown(s1, s2)
    # none of the policies is fired
    assert not s.need_shutdown()
    # 1st policy has to be fired
    s.update_state(BatchProcessingResult("foo", [object()], []))
    assert s.need_shutdown()


def test_and_policy():
    s1 = MaxTasksShutdown(max_tasks=1)
    s2 = MaxTasksShutdown(max_tasks=2)
    s = AndShutdown(s1, s2)
    # none of the policies is fired
    assert not s.need_shutdown()
    # 1st policy has to be fired, but it's not enough
    s.update_state(BatchProcessingResult("foo", [object()], []))
    assert not s.need_shutdown()
    # both policies are fired
    s.update_state(BatchProcessingResult("foo", [object()], []))
    assert s.need_shutdown()
