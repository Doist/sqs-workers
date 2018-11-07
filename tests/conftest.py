import datetime
import random
import string

import boto3
import localstack_client.session
import pytest
from sqs_workers import SQSEnv
from sqs_workers.memory_env import MemoryEnv


@pytest.fixture(scope='session', params=['aws', 'localstack', 'memory'])
def sqs_session(request):
    if request.param == 'aws':
        return boto3.Session()
    elif request.param == 'localstack':
        return localstack_client.session.Session()
    else:
        return None

@pytest.fixture(scope='session')
def sqs(sqs_session):
    if sqs_session is None:
        return MemoryEnv()
    queue_prefix = 'sqs_workers_tests_{:%Y%m%d}_'.format(
        datetime.datetime.utcnow())
    sqs = SQSEnv(session=sqs_session, queue_prefix=queue_prefix)
    return sqs


@pytest.fixture
def queue(sqs, random_queue_name):
    # type: (SQSEnv) -> string
    sqs.create_standard_queue(random_queue_name)
    yield random_queue_name
    sqs.delete_queue(random_queue_name)


@pytest.fixture
def queue_with_redrive(sqs, random_queue_name):
    # dead letter queue
    queue = random_queue_name
    dead_queue = random_queue_name + '_dead'
    sqs.create_standard_queue(dead_queue)

    # standard queue
    # 1 is the minimal value for redrive, and means
    # "put this to the dead letter queue after two failed attempts"
    sqs.create_standard_queue(queue,
                              redrive_policy=sqs.redrive_policy(dead_queue, 1))
    yield queue, dead_queue

    # delete all the queues
    sqs.delete_queue(queue)
    sqs.delete_queue(dead_queue)


@pytest.fixture
def fifo_queue(sqs, random_queue_name):
    # type: (SQSEnv) -> string
    queue_name = random_queue_name + '.fifo'
    sqs.create_fifo_queue(queue_name)
    yield queue_name
    sqs.delete_queue(queue_name)


@pytest.fixture
def random_queue_name():
    symbol = lambda: random.choice(string.ascii_lowercase + string.digits)
    return ''.join([symbol() for i in range(10)])
