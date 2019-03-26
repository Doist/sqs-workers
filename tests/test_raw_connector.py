import json

import pytest

from sqs_workers import IMMEDIATE_RETURN
from sqs_workers.memory_env import MemoryEnv
from sqs_workers.processors import DeadLetterProcessor

worker_results = {'say_hello': None, 'batch_say_hello': set()}


def raise_exception(username='Anonymous'):
    raise Exception('oops')


def say_hello_raw(row_message):
    # type: (str) -> None
    worker_results['say_hello'] = json.loads(row_message).get('username', None)


def kwargs_to_str(**kwargs):
    return json.dumps(kwargs)


@pytest.fixture(autouse=True)
def _reset_worker_results():
    global worker_results
    worker_results = {'say_hello': None, 'batch_say_hello': set()}


def test_add_raw_job(sqs, queue):
    sqs.add_raw_job(queue, 'say_hello', {})
    job_messages = sqs.get_sqs_messages(queue, 0)
    msg = job_messages[0]
    assert msg.body == 'say_hello'


def test_processor(sqs, queue):
    say_hello_task = sqs.processors.connect_raw(queue, say_hello_raw)
    say_hello_task.delay(kwargs_to_str(username='Homer'))
    assert worker_results['say_hello'] is None
    sqs.process_batch(queue, wait_seconds=0)
    assert worker_results['say_hello'] == 'Homer'


def test_process_raw_messages_once(sqs, queue):
    say_hello_task = sqs.processors.connect_raw(queue, say_hello_raw)
    say_hello_task.delay(kwargs_to_str(username='Homer'))

    processed = sqs.process_batch(queue, wait_seconds=0).succeeded_count()
    assert processed == 1
    processed = sqs.process_batch(queue, wait_seconds=0).succeeded_count()
    assert processed == 0


def test_copy_raw_processors(sqs, queue, queue2):
    # indirectly set the processor for queue2
    sqs.processors.connect_raw(queue, say_hello_raw)
    sqs.processors.copy(queue, queue2)

    # add job to that queue
    sqs.add_raw_job(queue2, kwargs_to_str(username='Homer'), {})

    # and see that it's succeeded
    processed = sqs.process_batch(queue2, wait_seconds=0).succeeded_count()
    assert processed == 1


def test_exception_returns_task_to_the_queue(sqs, queue):
    task = sqs.processors.connect_raw(
        queue, raise_exception, backoff_policy=IMMEDIATE_RETURN)
    task.delay(kwargs_to_str(username='Homer'))
    assert sqs.process_batch(queue, wait_seconds=0).failed_count() == 1

    # re-connect a non-broken processor for the queue
    sqs.processors.connect_raw(queue, say_hello_raw)
    assert sqs.process_batch(queue, wait_seconds=0).succeeded_count() == 1


def test_redrive(sqs, queue_with_redrive):
    if isinstance(sqs, MemoryEnv):
        pytest.skip('Redrive not implemented with MemoryEnv')

    queue, dead_queue = queue_with_redrive

    # add processor which fails to the standard queue
    task = sqs.processors.connect_raw(
        queue, raise_exception, backoff_policy=IMMEDIATE_RETURN)

    # add message to the queue and process it twice
    # the message has to be moved to dead letter queue
    task.delay(kwargs_to_str(username='Homer'))
    assert sqs.process_batch(queue, wait_seconds=0).succeeded_count() == 0
    assert sqs.process_batch(queue, wait_seconds=0).succeeded_count() == 0

    # add processor which succeeds
    sqs.processors.connect_raw(dead_queue, say_hello_raw)
    assert sqs.process_batch(dead_queue, wait_seconds=0).succeeded_count() == 1


def test_dead_letter_processor(sqs, queue_with_redrive):
    sqs.processors.fallback_processor_maker = DeadLetterProcessor
    queue, dead_queue = queue_with_redrive

    # dead queue doesn't have a processor, so a dead letter processor
    # will be fired, and it will mark the task as successful
    task = sqs.processors.connect_raw(
        queue, raise_exception, backoff_policy=IMMEDIATE_RETURN)
    task.delay(kwargs_to_str(username='Homer'))
    assert sqs.process_batch(dead_queue, wait_seconds=0).succeeded_count() == 0

    # queue has processor which succeeds (but we need to wait at least
    # 1 second for this task to appear here)
    sqs.processors.connect_raw(queue, say_hello_raw)
    assert sqs.process_batch(queue, wait_seconds=2).succeeded_count() == 1
