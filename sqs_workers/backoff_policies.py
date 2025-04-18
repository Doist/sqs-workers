import random


class BackoffPolicy:
    def get_visibility_timeout(self, message):
        raise NotImplementedError()


class ConstantBackoff(BackoffPolicy):
    """
    Backoff policy which always returns the message back to the queue
    immediately on failure.
    """

    def __init__(self, backoff_value: float = 0):
        self.backoff_value = backoff_value

    def get_visibility_timeout(self, message) -> float:
        return self.backoff_value


class ExponentialBackoff(BackoffPolicy):
    """
    Backoff policy which keeps the message hidden from the queue
    with an exponential backoff.
    """

    def __init__(
        self,
        base: float = 2,
        min_visibility_timeout: float = 0,
        max_visbility_timeout: float = 30 * 60,
    ) -> None:
        self.base = base  # in seconds
        self.min_visibility_timeout = min_visibility_timeout
        self.max_visibility_timeout = max_visbility_timeout

    def get_visibility_timeout(self, message) -> int:
        prev_receive_count = int(message.attributes["ApproximateReceiveCount"]) - 1
        mu = self.min_visibility_timeout + (self.base**prev_receive_count)
        sigma = float(mu) / 10
        visibility_timeout = random.normalvariate(mu, sigma)
        visibility_timeout = max(self.min_visibility_timeout, visibility_timeout)
        visibility_timeout = min(self.max_visibility_timeout, visibility_timeout)
        return round(visibility_timeout)


DEFAULT_BACKOFF = ExponentialBackoff()
IMMEDIATE_RETURN = ConstantBackoff()
