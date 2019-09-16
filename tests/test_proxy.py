def test_add_pickle_job(sqs, queue_name):
    messages = []

    _queue = sqs.queue(queue_name)

    @_queue.processor("say_hello")
    def say_hello(username=None):
        messages.append(username)

    say_hello.delay(username="Homer")
    result = sqs.process_batch(queue_name, wait_seconds=0)
    assert result.succeeded_count() == 1
    assert messages == ["Homer"]
