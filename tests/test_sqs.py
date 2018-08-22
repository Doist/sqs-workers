import pytest

from sqs_workers import IMMEDIATE_RETURN
from sqs_workers.codecs import PickleCodec, JSONCodec

worker_results = {'say_hello': None, 'batch_say_hello': set()}


def raise_exception(username='Anonymous'):
    raise Exception('oops')


def say_hello(username='Anonymous'):
    worker_results['say_hello'] = username


def batch_say_hello(messages):
    for msg in messages:
        worker_results['batch_say_hello'].add(msg['username'])


@pytest.fixture(autouse=True)
def _reset_worker_results():
    global worker_results
    worker_results = {'say_hello': None, 'batch_say_hello': set()}


def test_add_pickle_job(sqs, queue):
    sqs.add_job(queue, 'say_hello', username='Homer')
    job_messages = sqs.get_raw_messages(queue, 0)
    msg = job_messages[0]
    assert msg.message_attributes['JobName']['StringValue'] == 'say_hello'
    assert msg.message_attributes['ContentType']['StringValue'] == 'pickle'
    assert PickleCodec.deserialize(msg.body) == {'username': 'Homer'}


def test_add_json_job(sqs, queue):
    sqs.add_job(queue, 'say_hello', username='Homer', _content_type='json')
    job_messages = sqs.get_raw_messages(queue, 0)
    msg = job_messages[0]
    assert msg.message_attributes['JobName']['StringValue'] == 'say_hello'
    assert msg.message_attributes['ContentType']['StringValue'] == 'json'
    assert JSONCodec.deserialize(msg.body) == {'username': 'Homer'}


def test_processor(sqs, queue):
    say_hello_task = sqs.connect_processor(queue, 'say_hello', say_hello)
    say_hello_task.delay(username='Homer')
    assert worker_results['say_hello'] is None
    sqs.process_batch(queue, wait_seconds=0)
    assert worker_results['say_hello'] == 'Homer'


def test_process_messages_once(sqs, queue):
    say_hello_task = sqs.connect_processor(queue, 'say_hello', say_hello)
    say_hello_task.delay(username='Homer')
    processed = sqs.process_batch(queue, wait_seconds=0)
    assert processed == 1
    processed = sqs.process_batch(queue, wait_seconds=0)
    assert processed == 0


def test_batch_processor(sqs, queue):
    task = sqs.connect_batch_processor(queue, 'batch_say_hello',
                                       batch_say_hello)

    usernames = {'u{}'.format(i) for i in range(20)}

    # enqueue messages
    for username in usernames:
        task.delay(username=username)

    # sometimes SQS doesn't return all messages at once, and we need to drain
    # the queue with the infinite loop
    while True:
        processed = sqs.process_batch(queue, wait_seconds=0)
        if processed == 0:
            break

    assert worker_results['batch_say_hello'] == usernames


def test_arguments_validator_raises_exception_on_extra(sqs, queue):
    say_hello_task = sqs.connect_processor(queue, 'say_hello', say_hello)
    with pytest.raises(TypeError):
        say_hello_task.delay(username='Homer', foo=1)


def test_arguments_validator_adds_kwargs(sqs, queue):
    say_hello_task = sqs.connect_processor(queue, 'say_hello', say_hello)
    say_hello_task.delay()
    assert sqs.process_batch(queue, wait_seconds=0) == 1
    assert worker_results['say_hello'] == 'Anonymous'


def test_delay_accepts_converts_args_to_kwargs(sqs, queue):
    say_hello_task = sqs.connect_processor(queue, 'say_hello', say_hello)
    say_hello_task.delay('Homer')  # we don't use username="Homer"
    assert sqs.process_batch(queue, wait_seconds=0) == 1
    assert worker_results['say_hello'] == 'Homer'


def test_exception_returns_task_to_the_queue(sqs, queue):
    task = sqs.connect_processor(
        queue, 'say_hello', raise_exception, backoff_policy=IMMEDIATE_RETURN)
    task.delay(username='Homer')
    assert sqs.process_batch(queue, wait_seconds=0) == 0

    # re-connect a non-broken processor for the queue
    sqs.connect_processor(queue, 'say_hello', say_hello)
    assert sqs.process_batch(queue, wait_seconds=0) == 1


def test_redrive(sqs, queue_with_redrive):
    queue, dead_queue = queue_with_redrive

    # add processor which fails to the standard queue
    task = sqs.connect_processor(
        queue, 'say_hello', raise_exception, backoff_policy=IMMEDIATE_RETURN)

    # add message to the queue and process it twice
    # the message has to be moved to dead letter queue
    task.delay(username='Homer')
    assert sqs.process_batch(queue, wait_seconds=0) == 0
    assert sqs.process_batch(queue, wait_seconds=0) == 0

    # add processor which succeeds
    sqs.connect_processor(dead_queue, 'say_hello', say_hello)
    assert sqs.process_batch(dead_queue, wait_seconds=0) == 1
