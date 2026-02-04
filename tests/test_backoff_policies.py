import random
from unittest.mock import MagicMock

import pytest

from sqs_workers.backoff_policies import (
    ConstantBackoff,
    ExponentialBackoff,
)


@pytest.fixture
def mock_message():
    def _make_message(receive_count: int):
        message = MagicMock()
        message.attributes = {"ApproximateReceiveCount": str(receive_count)}
        return message

    return _make_message


class TestConstantBackoff:
    def test_returns_constant_value(self):
        policy = ConstantBackoff(backoff_value=30)
        message = MagicMock()
        assert policy.get_visibility_timeout(message) == 30

    def test_default_is_zero(self):
        policy = ConstantBackoff()
        message = MagicMock()
        assert policy.get_visibility_timeout(message) == 0


class TestExponentialBackoff:
    def test_default_parameters_backward_compatible(self, mock_message):
        policy = ExponentialBackoff()
        assert policy.base == 2
        assert policy.min_visibility_timeout == 0
        assert policy.max_visibility_timeout == 30 * 60
        assert policy.multiplier == 1

    def test_first_attempt_uses_base(self, mock_message):
        random.seed(42)
        policy = ExponentialBackoff(base=2, min_visibility_timeout=0, multiplier=1)
        timeout = policy.get_visibility_timeout(mock_message(1))
        assert timeout == 1

    def test_exponential_growth(self, mock_message):
        random.seed(42)
        policy = ExponentialBackoff(base=2, min_visibility_timeout=0, multiplier=1)
        timeouts = [policy.get_visibility_timeout(mock_message(i)) for i in range(1, 6)]
        for i in range(1, len(timeouts)):
            assert timeouts[i] >= timeouts[i - 1]

    def test_multiplier_scales_timeout(self, mock_message):
        random.seed(42)
        policy_no_mult = ExponentialBackoff(
            base=2, min_visibility_timeout=0, multiplier=1
        )
        timeout_no_mult = policy_no_mult.get_visibility_timeout(mock_message(3))

        random.seed(42)
        policy_with_mult = ExponentialBackoff(
            base=2, min_visibility_timeout=0, multiplier=10
        )
        timeout_with_mult = policy_with_mult.get_visibility_timeout(mock_message(3))

        assert timeout_with_mult == pytest.approx(timeout_no_mult * 10, rel=0.2)

    def test_min_visibility_timeout_is_floor(self, mock_message):
        random.seed(42)
        policy = ExponentialBackoff(base=2, min_visibility_timeout=60)
        timeout = policy.get_visibility_timeout(mock_message(1))
        assert timeout >= 60

    def test_max_visibility_timeout_is_ceiling(self, mock_message):
        random.seed(42)
        policy = ExponentialBackoff(
            base=10, min_visibility_timeout=0, max_visbility_timeout=100, multiplier=100
        )
        timeout = policy.get_visibility_timeout(mock_message(10))
        assert timeout <= 100

    def test_multiplier_combined_with_min_timeout(self, mock_message):
        random.seed(42)
        policy = ExponentialBackoff(
            base=2, min_visibility_timeout=10, multiplier=5, max_visbility_timeout=1000
        )
        timeout = policy.get_visibility_timeout(mock_message(3))
        assert timeout >= 10
