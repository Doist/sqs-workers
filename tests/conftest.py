import datetime
import random
import string

import boto3
import localstack_client.session
import pytest

from sqs_workers import SQSEnv, create_fifo_queue, create_standard_queue, delete_queue
from sqs_workers.memory_sqs import MemoryAWS, MemorySession


@pytest.fixture(scope="session", params=["aws", "localstack", "memory"])
def sqs_session(request):
    if request.param == "aws":
        return boto3.Session()
    elif request.param == "localstack":
        return localstack_client.session.Session()
    else:
        return MemorySession(MemoryAWS())


@pytest.fixture
def sqs(sqs_session):
    queue_prefix = "sqs_workers_tests_{:%Y%m%d}_".format(datetime.datetime.utcnow())
    sqs = SQSEnv(session=sqs_session, queue_prefix=queue_prefix)
    return sqs


@pytest.fixture
def queue_name(sqs_session, sqs, random_string):
    # type: (SQSEnv) -> string
    create_standard_queue(sqs, random_string)
    yield random_string
    delete_queue(sqs, random_string)


@pytest.fixture
def queue_name2(sqs_session, sqs, random_string):
    # type: (SQSEnv) -> string
    create_standard_queue(sqs, random_string + "_2")
    yield random_string + "_2"
    delete_queue(sqs, random_string + "_2")


@pytest.fixture
def queue_name_with_redrive(sqs_session, sqs, random_string):
    # dead letter queue_name
    queue = random_string
    dead_queue = random_string + "_dead"

    create_standard_queue(sqs, dead_queue)

    # standard queue_name
    # 1 is the minimal value for redrive, and means
    # "put this to the dead letter queue_name after two failed attempts"
    create_standard_queue(sqs, queue, redrive_policy=sqs.redrive_policy(dead_queue, 1))

    yield queue, dead_queue

    # delete all the queues
    delete_queue(sqs, queue)
    delete_queue(sqs, dead_queue)


@pytest.fixture
def fifo_queue_name(sqs_session, sqs, random_string):
    # type: (SQSEnv) -> string
    queue_name = random_string + ".fifo"
    create_fifo_queue(sqs, queue_name)
    yield queue_name
    delete_queue(sqs, queue_name)


@pytest.fixture
def random_string():
    return "".join([symbol() for i in range(10)])


def symbol():
    return random.choice(string.ascii_lowercase + string.digits)
