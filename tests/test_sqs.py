import time

import pytest

from sqs_workers import (
    IMMEDIATE_RETURN,
    ExponentialBackoff,
    create_fifo_queue,
    create_standard_queue,
    delete_queue,
)
from sqs_workers.codecs import JSONCodec, PickleCodec
from sqs_workers.deadletter_queue import DeadLetterQueue
from sqs_workers.memory_sqs import MemorySession
from sqs_workers.processors import Processor
from sqs_workers.queue import RawQueue

worker_results = {"say_hello": None}


def raise_exception(username="Anonymous"):
    raise Exception("oops")


def say_hello(username="Anonymous"):
    worker_results["say_hello"] = username


@pytest.fixture(autouse=True)
def _reset_worker_results():
    global worker_results
    worker_results = {"say_hello": None}


def test_add_pickle_job(sqs, queue_name):
    queue = sqs.queue(queue_name)
    queue.add_job("say_hello", username="Homer")
    job_messages = queue.get_raw_messages(0)
    msg = job_messages[0]
    assert msg.message_attributes["JobName"]["StringValue"] == "say_hello"
    assert msg.message_attributes["ContentType"]["StringValue"] == "pickle"
    assert PickleCodec.deserialize(msg.body) == {"username": "Homer"}


def test_add_json_job(sqs, queue_name):
    queue = sqs.queue(queue_name)
    queue.add_job("say_hello", username="Homer", _content_type="json")
    job_messages = queue.get_raw_messages(0)
    msg = job_messages[0]
    assert msg.message_attributes["JobName"]["StringValue"] == "say_hello"
    assert msg.message_attributes["ContentType"]["StringValue"] == "json"
    assert JSONCodec.deserialize(msg.body) == {"username": "Homer"}


def test_processor(sqs, queue_name):
    queue = sqs.queue(queue_name)
    say_hello_task = queue.connect_processor("say_hello", say_hello)
    say_hello_task.delay(username="Homer")
    assert worker_results["say_hello"] is None
    queue.process_batch(wait_seconds=0)
    assert worker_results["say_hello"] == "Homer"


def test_baked_tasks(sqs, queue_name):
    queue = sqs.queue(queue_name)
    say_hello_task = queue.connect_processor("say_hello", say_hello)
    baked_task = say_hello_task.bake(username="Homer")
    baked_task.delay()
    queue.process_batch(wait_seconds=0)
    assert worker_results["say_hello"] == "Homer"


def test_batch_should_keep_messages_on_add(sqs, queue_name):
    queue = sqs.queue(queue_name)
    say_hello_task = queue.connect_processor("say_hello", say_hello)

    # no message before
    assert len(queue.get_raw_messages(0)) == 0

    with say_hello_task.batch():
        say_hello_task.delay(username="Lisa")

        # no message yet
        assert len(queue.get_raw_messages(0)) == 0


def test_batch_should_flush_on_exit(sqs, queue_name):
    queue = sqs.queue(queue_name)
    say_hello_task = queue.connect_processor("say_hello", say_hello)

    with say_hello_task.batch():
        say_hello_task.delay(username="Lisa")
        say_hello_task.delay(username="Bart")

        # not enough tasks to flush the batch yet
        assert len(queue.get_raw_messages(0)) == 0

    # but once the batch is closed, all tasks are added
    assert len(queue.get_raw_messages(0)) == 2


def test_batch_should_keep_messages_until_overflow(sqs, queue_name):
    queue = sqs.queue(queue_name)
    say_hello_task = queue.connect_processor("say_hello", say_hello)

    with say_hello_task.batch():
        # no messages after 9 tasks
        for n in range(9):
            say_hello_task.delay(username="Homer %d" % n)
        assert len(queue.get_raw_messages(0)) == 0

        # 2 more: overflow, the first 10 messages are added to the queue
        say_hello_task.delay(username="Bart")
        say_hello_task.delay(username="Lisa")
        assert len(queue.get_raw_messages(0)) == 10

    # 1 message remaining: it's added once the batch is closed
    assert len(queue.get_raw_messages(0)) == 1


def test_call_raises_exception(sqs, queue_name):
    queue = sqs.queue(queue_name)
    say_hello_task = queue.connect_processor("say_hello", say_hello)
    with pytest.raises(RuntimeError):
        say_hello_task(username="Homer")


def test_run_executes_task_immediately(sqs, queue_name):
    queue = sqs.queue(queue_name)
    say_hello_task = queue.connect_processor("say_hello", say_hello)
    say_hello_task.run(username="Homer")
    assert worker_results["say_hello"] == "Homer"


def test_process_messages_once(sqs, queue_name):
    queue = sqs.queue(queue_name)
    say_hello_task = queue.connect_processor("say_hello", say_hello)
    say_hello_task.delay(username="Homer")
    processed = queue.process_batch(wait_seconds=0).succeeded_count()
    assert processed == 1
    processed = queue.process_batch(wait_seconds=0).succeeded_count()
    assert processed == 0


def test_copy_processors(sqs, queue_name, queue_name2):
    queue = sqs.queue(queue_name)
    queue2 = sqs.queue(queue_name2)

    # indirectly set the processor for queue_name2
    queue.connect_processor("say_hello", say_hello)
    queue.copy_processors(queue2)

    # add job to that queue_name
    queue2.add_job("say_hello")

    # and see that it's succeeded
    processed = queue2.process_batch(wait_seconds=0).succeeded_count()
    assert processed == 1


def test_arguments_validator_raises_exception_on_extra(sqs, queue_name):
    queue = sqs.queue(queue_name)
    say_hello_task = queue.connect_processor("say_hello", say_hello)
    with pytest.raises(TypeError):
        say_hello_task.delay(username="Homer", foo=1)


def test_arguments_validator_adds_kwargs(sqs, queue_name):
    queue = sqs.queue(queue_name)
    say_hello_task = queue.connect_processor("say_hello", say_hello)
    say_hello_task.delay()
    assert queue.process_batch(wait_seconds=0).succeeded_count() == 1
    assert worker_results["say_hello"] == "Anonymous"


def test_delay_accepts_converts_args_to_kwargs(sqs, queue_name):
    queue = sqs.queue(queue_name)
    say_hello_task = queue.connect_processor("say_hello", say_hello)
    say_hello_task.delay("Homer")  # we don't use username="Homer"
    assert queue.process_batch(wait_seconds=0).succeeded_count() == 1
    assert worker_results["say_hello"] == "Homer"


def test_exception_returns_task_to_the_queue(sqs, queue_name):
    queue = sqs.queue(queue_name, backoff_policy=IMMEDIATE_RETURN)
    task = queue.connect_processor("say_hello", raise_exception)
    task.delay(username="Homer")
    assert queue.process_batch(wait_seconds=0).failed_count() == 1

    # re-connect a non-broken processor for the queue_name
    queue.connect_processor("say_hello", say_hello)
    assert queue.process_batch(wait_seconds=0).succeeded_count() == 1


def test_redrive(sqs_session, sqs, queue_name_with_redrive):
    if isinstance(sqs_session, MemorySession):
        pytest.skip("Redrive not implemented with MemorySession")

    queue_name, dead_queue_name = queue_name_with_redrive
    queue = sqs.queue(queue_name, backoff_policy=IMMEDIATE_RETURN)
    dead_queue = sqs.queue(dead_queue_name)

    # add processor which fails to the standard queue_name
    task = queue.connect_processor("say_hello", raise_exception)

    # add message to the queue_name and process it twice
    # the message has to be moved to dead letter queue_name
    task.delay(username="Homer")
    assert queue.process_batch(wait_seconds=0).succeeded_count() == 0
    assert queue.process_batch(wait_seconds=0).succeeded_count() == 0

    # add processor which succeeds
    dead_queue.connect_processor("say_hello", say_hello)
    assert dead_queue.process_batch(wait_seconds=0).succeeded_count() == 1


def test_deadletter_processor(sqs, queue_name_with_redrive):
    queue_name, dead_queue_name = queue_name_with_redrive
    queue = sqs.queue(queue_name, RawQueue)
    dead_queue = sqs.queue(
        dead_queue_name, DeadLetterQueue.maker(upstream_queue=queue)
    )  # type: DeadLetterQueue

    dead_queue.add_raw_job("say_hello")
    assert dead_queue.process_batch(wait_seconds=0).succeeded_count() == 1

    # queue_name has processor which succeeds (but we need to wait at least
    # 1 second for this task to appear here)
    message_body = {}

    def raw_processor(message):
        message_body["body"] = message.body

    queue.connect_raw_processor(raw_processor)
    assert queue.process_batch(wait_seconds=2).succeeded_count() == 1


def test_exponential_backoff_works(sqs, queue_name):
    queue = sqs.queue(
        queue_name, backoff_policy=ExponentialBackoff(0.1, max_visbility_timeout=0.1)
    )
    task = queue.connect_processor("say_hello", raise_exception)
    task.delay(username="Homer")
    assert queue.process_batch(wait_seconds=0).failed_count() == 1


def test_drain_queue(sqs, queue_name):
    queue = sqs.queue(queue_name)
    say_hello_task = queue.connect_processor("say_hello", say_hello)
    say_hello_task.delay(username="One")
    say_hello_task.delay(username="Two")
    queue.drain_queue(wait_seconds=0)
    assert queue.process_batch(wait_seconds=0).succeeded_count() == 0
    assert worker_results["say_hello"] is None


def test_message_retention_period(sqs_session, sqs, random_string):
    if isinstance(sqs_session, MemorySession):
        pytest.skip("Message Retention id not implemented with MemorySession")

    try:
        create_standard_queue(sqs, random_string, message_retention_period=600)
        create_fifo_queue(sqs, random_string + ".fifo", message_retention_period=600)
    finally:
        try:
            delete_queue(sqs, random_string)
        except Exception:
            pass
        try:
            delete_queue(sqs, random_string + ".fifo")
        except Exception:
            pass


def test_deduplication_id(sqs_session, sqs, fifo_queue_name):
    if isinstance(sqs_session, MemorySession):
        pytest.skip("Deduplication id not implemented with MemorySession")

    fifo_queue = sqs.queue(fifo_queue_name)

    say_hello_task = fifo_queue.connect_processor("say_hello", say_hello)
    say_hello_task.delay(username="One", _deduplication_id="x")
    say_hello_task.delay(username="Two", _deduplication_id="x")
    assert fifo_queue.process_batch(wait_seconds=0).succeeded_count() == 1
    assert worker_results["say_hello"] == "One"


def test_group_id(sqs_session, sqs, fifo_queue_name):
    # not much we can test here, but at least test that it doesn't blow up
    # and that group_id and deduplication_id are orthogonal
    if isinstance(sqs_session, MemorySession):
        pytest.skip("GroupId id not implemented with MemorySession")

    fifo_queue = sqs.queue(fifo_queue_name)
    say_hello_task = fifo_queue.connect_processor("say_hello", say_hello)
    say_hello_task.delay(username="One", _deduplication_id="x", _group_id="g1")
    say_hello_task.delay(username="Two", _deduplication_id="x", _group_id="g2")
    assert fifo_queue.process_batch(wait_seconds=0).succeeded_count() == 1
    assert worker_results["say_hello"] == "One"


def test_delay_seconds(sqs, queue_name):
    queue = sqs.queue(queue_name)
    say_hello_task = queue.connect_processor("say_hello", say_hello)
    say_hello_task.delay(username="Homer", _delay_seconds=2)
    assert queue.process_batch(wait_seconds=1).succeeded_count() == 0
    time.sleep(3)
    assert queue.process_batch(wait_seconds=1).succeeded_count() == 1


def test_visibility_timeout(sqs, random_string):
    try:
        create_standard_queue(sqs, random_string, visibility_timeout=1)
        create_fifo_queue(sqs, random_string + ".fifo", visibility_timeout=1)
    finally:
        try:
            delete_queue(sqs, random_string)
        except Exception:
            pass
        try:
            delete_queue(sqs, random_string + ".fifo")
        except Exception:
            pass


def test_custom_processor(sqs, queue_name):
    queue = sqs.queue(queue_name)

    class CustomProcessor(Processor):
        def process(self, job_kwargs, job_context):
            job_kwargs["username"] = "Foo"
            super(CustomProcessor, self).process(job_kwargs, job_context)

    sqs.processor_maker = CustomProcessor
    say_hello_task = queue.connect_processor("say_hello", say_hello)
    say_hello_task.delay()
    assert queue.process_batch().succeeded_count() == 1
    assert worker_results["say_hello"] == "Foo"


def test_raw_queue(sqs, queue_name):
    message_body = {}

    def raw_processor(message):
        message_body["body"] = message.body

    queue = sqs.queue(queue_name, RawQueue)  # type: RawQueue
    queue.connect_raw_processor(raw_processor)
    queue.add_raw_job("foo")
    assert queue.process_batch().succeeded_count() == 1
    assert message_body["body"] == "foo"


def test_raw_queue_decorator(sqs, queue_name):
    message_body = {}

    queue = sqs.queue(queue_name, RawQueue)  # type: RawQueue

    @queue.raw_processor()
    def raw_processor(message):
        message_body["body"] = message.body

    queue.add_raw_job("foo")
    assert queue.process_batch().succeeded_count() == 1
    assert message_body["body"] == "foo"
