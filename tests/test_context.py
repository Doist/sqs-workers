import pytest

worker_results = {'say_hello': None, 'batch_say_hello': set()}


def say_hello_ctx(username='Anonymous', context=None):
    worker_results['say_hello'] = username, context.get('remote_addr')


def batch_say_hello_ctx(messages, context):
    for msg, ctx in zip(messages, context):
        worker_results['batch_say_hello'].add((msg['username'],
                                               ctx.get('remote_addr')))


@pytest.fixture(autouse=True)
def _reset_worker_results():
    global worker_results
    worker_results = {'say_hello': None, 'batch_say_hello': set()}


def test_context(sqs, queue):
    sqs.processors.connect(
        queue, 'say_hello', say_hello_ctx, pass_context=True)
    sqs.context['remote_addr'] = '127.0.0.1'
    sqs.add_job(queue, 'say_hello', username='Homer')
    sqs.process_batch(queue)
    assert worker_results['say_hello'] == ('Homer', '127.0.0.1')


def test_context_with(sqs, queue):
    sqs.processors.connect(
        queue, 'say_hello', say_hello_ctx, pass_context=True)
    with sqs.context(remote_addr='127.0.0.2'):
        sqs.add_job(queue, 'say_hello', username='Homer')
    sqs.process_batch(queue)
    assert worker_results['say_hello'] == ('Homer', '127.0.0.2')


def test_batch_context(sqs, queue):
    sqs.processors.connect_batch(
        queue, 'batch_say_hello', batch_say_hello_ctx, pass_context=True)
    sqs.context['remote_addr'] = '127.0.0.1'
    sqs.add_job(queue, 'batch_say_hello', username='Homer')
    sqs.add_job(queue, 'batch_say_hello', username='Bart')
    sqs.context['remote_addr'] = '127.0.0.2'
    sqs.add_job(queue, 'batch_say_hello', username='Lisa')

    # sometimes SQS doesn't return all messages at once, and we need to drain
    # the queue with the infinite loop
    while True:
        processed = sqs.process_batch(queue, wait_seconds=0).succeeded_count()
        if processed == 0:
            break

    assert worker_results['batch_say_hello'] == {
        ('Homer', '127.0.0.1'),
        ('Bart', '127.0.0.1'),
        ('Lisa', '127.0.0.2'),
    }
