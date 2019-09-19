import pytest

worker_results = {"say_hello": None}


def say_hello_ctx(username="Anonymous", context=None):
    worker_results["say_hello"] = username, context.get("remote_addr")


@pytest.fixture(autouse=True)
def _reset_worker_results():
    global worker_results
    worker_results = {"say_hello": None}


def test_context(sqs, queue_name):
    queue = sqs.queue(queue_name)
    queue.connect_processor("say_hello", say_hello_ctx, pass_context=True)
    sqs.context["remote_addr"] = "127.0.0.1"
    queue.add_job("say_hello", username="Homer")
    queue.process_batch()
    assert worker_results["say_hello"] == ("Homer", "127.0.0.1")


def test_context_with(sqs, queue_name):
    queue = sqs.queue(queue_name)
    queue.connect_processor("say_hello", say_hello_ctx, pass_context=True)
    with sqs.context(remote_addr="127.0.0.2"):
        queue.add_job("say_hello", username="Homer")
    queue.process_batch()
    assert worker_results["say_hello"] == ("Homer", "127.0.0.2")
