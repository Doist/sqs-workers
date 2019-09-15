import datetime

now = datetime.datetime.utcnow


class NeverShutdown(object):
    """
    Never shutdown the worker
    """

    def update_state(self, batch_processing_result):
        pass

    def need_shutdown(self):
        return False

    def __repr__(self):
        return "NeverShutdown()"


class IdleShutdown(object):
    """
    Shutdown worker if it's idle for certain time (set in seconds)
    """

    def __init__(self, idle_seconds):
        self.idle_seconds = idle_seconds
        self._idle_delta = datetime.timedelta(seconds=idle_seconds)
        self._is_idle = False
        self._last_seen = now()

    def update_state(self, batch_processing_result):
        """
        Update internal state of the shutdown policy
        """
        if batch_processing_result.total_count() == 0:
            self._is_idle = True
        else:
            self._is_idle = False
            self._last_seen = now()

    def need_shutdown(self):
        if not self._is_idle:
            return False
        return now() - self._last_seen >= self._idle_delta

    def __repr__(self):
        return "IdleShutdown({})".format(self.idle_seconds)


class MaxTasksShutdown(object):
    """
    Shutdown worker if it executed more than max_tasks in total (both
    successfully or with error)
    """

    def __init__(self, max_tasks):
        self.max_tasks = max_tasks
        self._tasks = 0

    def update_state(self, batch_processing_result):
        self._tasks += batch_processing_result.total_count()

    def need_shutdown(self):
        return self._tasks >= self.max_tasks

    def __repr__(self):
        return "MaxTasksShutdown({})".format(self.max_tasks)


class OrShutdown(object):
    """
    Return True if any of conditions of policies is met
    """

    def __init__(self, *policies):
        self.policies = policies

    def update_state(self, batch_processing_result):
        for p in self.policies:
            p.update_state(batch_processing_result)

    def need_shutdown(self):
        for p in self.policies:
            if p.need_shutdown():
                return True
        return False

    def __repr__(self):
        repr_policies = ", ".join([repr(p) for p in self.policies])
        return "OrShutdown({})".format(repr_policies)


class AndShutdown(object):
    """
    Return True if all of conditions of policies are met
    """

    def __init__(self, *policies):
        self.policies = policies

    def update_state(self, batch_processing_result):
        for p in self.policies:
            p.update_state(batch_processing_result)

    def need_shutdown(self):
        for p in self.policies:
            if not p.need_shutdown():
                return False
        return True

    def __repr__(self):
        repr_policies = ", ".join([repr(p) for p in self.policies])
        return "AndShutdown({})".format(repr_policies)


NEVER_SHUTDOWN = NeverShutdown()
ASAP_SHUTDOWN = IdleShutdown(0)
