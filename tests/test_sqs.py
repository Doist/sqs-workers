import pytest
import time
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
    job_messages = sqs.get_raw_messages(queue, None, 0)
    msg = job_messages[0]
    assert msg.message_attributes['JobName']['StringValue'] == 'say_hello'
    assert msg.message_attributes['ContentType']['StringValue'] == 'pickle'
    assert PickleCodec.deserialize(msg.body) == {'username': 'Homer'}


def test_add_json_job(sqs, queue):
    sqs.add_job(queue, 'say_hello', username='Homer', _content_type='json')
    job_messages = sqs.get_raw_messages(queue, None, 0)
    msg = job_messages[0]
    assert msg.message_attributes['JobName']['StringValue'] == 'say_hello'
    assert msg.message_attributes['ContentType']['StringValue'] == 'json'
    assert JSONCodec.deserialize(msg.body) == {'username': 'Homer'}


def test_processor(sqs, queue):
    say_hello_task = sqs.connect_processor(queue, 'say_hello', say_hello)
    say_hello_task.delay(username='Homer')
    assert worker_results['say_hello'] is None
    sqs.process_queue(queue, exit_on_empty=True)
    assert worker_results['say_hello'] == 'Homer'


def test_process_messages_once(sqs, queue):
    say_hello_task = sqs.connect_processor(queue, 'say_hello', say_hello)
    say_hello_task.delay(username='Homer')
    processed = sqs.process_queue(queue, exit_on_empty=True)
    assert processed == 1
    processed = sqs.process_queue(queue, exit_on_empty=True)
    assert processed == 0


def test_batch_processor(sqs, queue):
    task = sqs.connect_batch_processor(
        queue, 'batch_say_hello', batch_say_hello)
    task.delay(username='Homer')
    task.delay(username='Marge')
    task.delay(username='Bart')
    task.delay(username='Lisa')

    processed = sqs.process_queue(queue, exit_on_empty=True)
    assert processed == 4
    assert worker_results['batch_say_hello'] == {'Homer', 'Marge', 'Bart', 'Lisa'}


def test_process_multiple_queues(sqs, queue):
    say_hello_task = sqs.connect_processor(queue, 'say_hello', say_hello)
    say_hello_task.delay(username='Homer')
    sqs.process_queues([queue], exit_on_empty=True)
    # ensure that there's nothing left by calling "process_queue"
    assert sqs.process_queue(queue, exit_on_empty=True) == 0


def test_arguments_validator_ignores_extra(sqs, queue):
    say_hello_task = sqs.connect_processor(queue, 'say_hello', say_hello)
    say_hello_task.delay(username='Homer', foo=1)
    assert sqs.process_queue(queue, exit_on_empty=True) == 1


def test_arguments_validator_adds_kwargs(sqs, queue):
    say_hello_task = sqs.connect_processor(queue, 'say_hello', say_hello)
    say_hello_task.delay()
    assert sqs.process_queue(queue, exit_on_empty=True) == 1
    assert worker_results['say_hello'] == 'Anonymous'


def test_exception_keeps_task_in_the_queue(sqs, queue):
    task = sqs.connect_processor(queue, 'say_hello', raise_exception)
    task.delay(username='Homer')
    assert sqs.process_queue(queue, visibility_timeout=2, exit_on_empty=True) == 0

    # re-connect a non-broken processor for the queue
    sqs.connect_processor(queue, 'say_hello', say_hello)

    # failed task is not there yet
    assert sqs.process_queue(queue, exit_on_empty=True) == 0

    # wait more than 2 seconds (sometimes it can take a while)
    for i in range(60):
        time.sleep(1)
        if sqs.process_queue(queue, exit_on_empty=True) == 1:
            return
    else:
        assert False, "Task is not returned to the queue after 60 seconds"
