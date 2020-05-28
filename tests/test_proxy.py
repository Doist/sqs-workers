def test_add_pickle_job(sqs, queue_name):
    messages = []

    _queue = sqs.queue(queue_name)

    @_queue.processor("say_hello")
    def say_hello(username=None):
        messages.append(username)

    say_hello.delay(username="Homer")
    result = sqs.queue(queue_name).process_batch(wait_seconds=0)
    assert result.succeeded_count() == 1
    assert messages == ["Homer"]


def test_debug_mode(sqs, queue_name):
    messages = []

    sqs.debug_mode = True
    _queue = sqs.queue(queue_name)

    @_queue.processor("say_hello")
    def say_hello(username=None):
        messages.append(username)

    say_hello.delay(username="Homer")
    assert messages == ["Homer"]
