import pytest

worker_results = {"say_hello": None}


def say_hello_ctx(username="Anonymous", context=None):
    worker_results["say_hello"] = username, context.get("remote_addr")


@pytest.fixture(autouse=True)
def _reset_worker_results():
    global worker_results
    worker_results = {"say_hello": None}


def test_context(sqs, queue_name):
    sqs.processors.connect(queue_name, "say_hello", say_hello_ctx, pass_context=True)
    sqs.context["remote_addr"] = "127.0.0.1"
    sqs.queue(queue_name).add_job("say_hello", username="Homer")
    sqs.process_batch(queue_name)
    assert worker_results["say_hello"] == ("Homer", "127.0.0.1")


def test_context_with(sqs, queue_name):
    sqs.processors.connect(queue_name, "say_hello", say_hello_ctx, pass_context=True)
    with sqs.context(remote_addr="127.0.0.2"):
        sqs.queue(queue_name).add_job("say_hello", username="Homer")
    sqs.process_batch(queue_name)
    assert worker_results["say_hello"] == ("Homer", "127.0.0.2")
