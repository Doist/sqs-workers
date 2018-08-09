import pytest
import datetime
import random
import string
from sqs_workers import SQSEnv


@pytest.fixture(scope='session')
def sqs():
    # type: () -> SQSEnv
    queue_prefix = 'sqs_workers_tests_{:%Y%m%d}_'.format(
        datetime.datetime.utcnow())
    sqs = SQSEnv(queue_prefix=queue_prefix)
    yield sqs
    for queue in sqs.get_all_known_queues():
        sqs.delete_queue(queue)


@pytest.fixture
def queue(sqs):
    # type: (SQSEnv) -> string
    queue_name = get_queue_name()
    sqs.create_standard_queue(queue_name)
    yield queue_name
    sqs.delete_queue(queue_name)


@pytest.fixture
def fifo_queue(sqs):
    # type: (SQSEnv) -> string
    queue_name = get_queue_name() + '.fifo'
    sqs.create_fifo_queue(queue_name)
    yield queue_name
    sqs.delete_queue(queue_name)


def get_queue_name():
    symbol = lambda: random.choice(string.ascii_lowercase + string.digits)
    return ''.join([symbol() for i in range(10)])
