# SQS Workers

An opinionated queue processor for Amazon SQS.

## Usage

Install the package with

```bash
pip install sqs-workers
```

Configure your boto3 library to provide access requisites for your installation with [something like this](https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration):

```bash
aws configure
```

Don't forget to set your preferred AWS region.

Then you will start managing two systems (most likely, from the same codebase): one of them adds messages to the queue, and another one executes them.

```python
from sqs_workers import SQSEnv, create_standard_queue

# This environment will use AWS requisites from ~/.aws/ directory
sqs = SQSEnv()

# Create a new queue.
# Note that you can use the AWS web interface for the same action as well, the
# web interface provides more options. You only need to do it once.
create_standard_queue(sqs, "emails")

# Get the queue object
queue = sqs.queue("emails")

# Register a queue processor
@queue.processor("send_email")
def send_email(to, subject, body):
    print(f"Sending email {subject} to {to}")
```

Then there are two ways of adding tasks to the queue. Classic (aka "explicit"):

```python
queue.add_job("send_email", to="user@example.com", subject="Hello world", body="hello world")
```

And the "Celery way" (we mimic the Celery API to some extent)

```python
send_email.delay(to="user@example.com", subject="Hello world", body="hello world")
```

To process the queue, you have to run workers manually. Create a new file which will contain the definition of the sqs object and register all processors (most likely, by importing necessary modules from your project), and then run SQS

```python
from sqs_workers import SQSEnv
sqs = SQSEnv()
...
sqs.queue('emails').process_queue()
```

In production, we usually don't handle multiple queues in the same process, but for the development environment it's easier to tackle with all the queues at once with

```python
sqs.process_queues()
```

## Serialization

There are two serializers: JSON and pickle.

## Baked tasks

You can create so-called "baked async tasks," entities which, besides the task itself, contain arguments which have to be used to call the task.

Think of baked tasks as of light version of [Celery signatures](http://docs.celeryproject.org/en/latest/userguide/canvas.html#signatures)

```python
task = send_email.bake(to='user@example.com', subject='Hello world', body='hello world')
task.delay()
```

Is the same as

```python
send_email.delay(to='user@example.com', subject='Hello world', body='hello world')
```

## Batch Writes

If you have many tasks to enqueue, it may be more efficient to use batching when adding them:

```python
# Classic ("explicit") API
with queue.add_batch():
    queue.add_job("send_email", to="user1@example.com", subject="Hello world", body="hello world")
    queue.add_job("send_email", to="user2@example.com", subject="Hello world", body="hello world")

# "Celery way"
with send_email.batch():
    send_email.delay(to="user1@example.com", subject="Hello world", body="hello world")
    send_email.delay(to="user2@example.com", subject="Hello world", body="hello world")
```

When batching is enabled:

- tasks are added to SQS by batches of 10, reducing the number of AWS operations
- it is not possible to get the task `MessageId`, as it is not known until the batch is sent
- care has to be taken about message size, as SQS limits both the individual message size and the maximum total payload size to 256 kB.

## Batch Reads

If you wish to consume and process batches of messages from a queue at once (say to speed up ingestion)
you can set the `batching_policy` at queue level.

The underlying function is expected to have a single parameter which will receive the list of messages.

```python
from sqs_workers.batching import BatchMessages
from sqs_workers import SQSEnv, create_standard_queue

sqs = SQSEnv()

create_standard_queue(sqs, "emails")

queue = sqs.queue("emails", batching_policy=BatchMessages(batch_size=10))

@queue.processor("send_email")
def send_email(messages: list):
    for email in messages:
        print(f"Sending email {email['subject']} to {email['to']}")
```

This function will receive up to 10 messages at once based on:

* How many are available on the queue being consumed
* How many of those messages on the `emails` queue are for the `send_email` job

## Synchronous task execution

In Celery, you can run any task synchronously if you just call it as a function with arguments. Our AsyncTask raises a RuntimeError for this case.

```python
send_email(to='user@example.com', subject='Hello world', body='hello world')
...
RuntimeError: Async task email.send_email called synchronously (probably,
by mistake). Use either AsyncTask.run(...) to run the task synchronously
or AsyncTask.delay(...) to add it to the queue.
```

If you want to run a task synchronously, use `run()` method of the task.

```
send_email.run(to='user@example.com', subject='Hello world', body='hello world')
```

## FIFO queues

Fifo queues can be created with `create_fifo_queue` and have to have the name, which ends with ".fifo".

```python
from sqs_workers import SQSEnv, create_fifo_queue
sqs = SQSEnv()

create_fifo_queue(sqs, 'emails_dead.fifo')
create_fifo_queue(sqs, 'emails.fifo',
    redrive_policy=sqs.redrive_policy('emails_dead.fifo', 3)
)
```

Unless the flag `content_based_deduplication` is set, every message has to be sent with an attribute `_deduplication_id`. By default, all messages have the same message group `default`, but you can change it with `_group_id`.

```python
sqs.queue("emails.fifo").add_job(
    'send_email', _deduplication_id=subject, _group_id=email, **kwargs)
```

[More about FIFO queues on AWS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html)

## Exception processing

If task processing ended up with an exception, the error is logged, and the task is returned back to the queue after a while. The exact behavior is defined by queue settings.

## Custom processors

You can define your own processor if you need to perform some specific actions before of after executing a particular task.

Example for the custom processor

```python
from sqs_workers import SQSEnv
from sqs_workers.processors import Processor

class CustomProcessor(Processor):
    def process(self, job_kwargs):
        print(f'Processing {self.queue_name}.{self.job_name} with {job_kwargs}')
        super(CustomProcessor, self).process(job_kwargs)

sqs = SQSEnv(processor_maker=CustomProcessor)
```

## Working with contexts

Context is implicitly passed to the worker via the job message and can be used for logging or profiling purposes.

Usage example.

```python
queue = sqs.queue("q1")

@queue.processor('q1', 'hello_world', pass_context=True)
def hello_world(username=None, context=None):
    print(f'Hello {username} from {context["remote_addr"]}')

with sqs.context(remote_addr='127.0.0.1'):
    hello_world.delay(username='Foo')

queue.process_batch()
```

Alternatively, you can set a context like this.

```python
sqs.context['remote_addr'] = '127.0.0.1'
hello_world.delay(username='Foo')
```

And then, when the context needs to be cleared:

```python
sqs.context.clear()
```

In a web application, you usually call it at the end of the processing of the web request.

## Automatic applying of the context for all tasks

Instead of dealing with the context inside every processing function, you can perform this in processors by subclassing them.

```python
import os
from sqs_workers import SQSEnv
from sqs_workers.processors import Processor

class CustomProcessor(Processor):
    def process(self, job_kwargs, job_context):
        os.environ['REMOTE_ADDR'] = job_context['remote_addr']
        super(CustomProcessor, self).process(job_kwargs, job_context)
        os.unsetenv('REMOTE_ADDR')

sqs = SQSEnv(
    processor_maker=CustomProcessor,
)
```

## Raw queues

Raw queues can have only one processor, which should be a function, accepting the message as its only argument.

Raw queues are helpful to process messages, added to SQS from external sources, such as CloudWatch events.

You start the processor the same way, creating a new standard queue if needed.

```python
from sqs_workers import SQSEnv, create_standard_queue
sqs = SQSEnv()
create_standard_queue(sqs, 'cron')
```

Then you get a queue, but provide a queue_maker parameter to it, to create a queue of the necessary type, and define a processor for it.

```python
from sqs_workers import RawQueue

cron = sqs.queue('cron', RawQueue)

@cron.raw_processor()
def processor(message):
    print(message.body)
```

Then start processing your queue as usual:

```python
cron.process_queue()
```

You can also send raw messages to the queue, but this is probably less useful:

```python
cron.add_raw_job("Hello world")
```

## Processing Messages from CloudWatch

By default message body by CloudWatch scheduler has following JSON structure.

```json
{
  "version": "0",
  "id": "a9a10406-9a1f-0ddc-51ae-08db551fac42",
  "detail-type": "Scheduled Event",
  "source": "aws.events",
  "account": "NNNNNNNNNN",
  "time": "2019-09-20T09:19:56Z",
  "region": "eu-west-1",
  "resources": ["arn:aws:events:eu-west-1:NNNNNNNNNN:rule/Playground"],
  "detail": {}
}
```

Headers of the message:

```python
{
    'SenderId': 'AIDAJ2E....',
    'ApproximateFirstReceiveTimestamp': '1568971264367',
    'ApproximateReceiveCount': '1',
    'SentTimestamp': '1568971244845',
}
```

You can pass any valid JSON as a message, though, and it will be passed as-is to the message body. Something like this:

```json
{ "message": "Hello world" }
```

## Dead-letter queues and redrive

On creating the queue, you can set the fallback dead-letter queue and redrive policy, which can look like this.

```python
from sqs_workers import SQSEnv, create_standard_queue
sqs = SQSEnv()

create_standard_queue(sqs, 'emails_dead')
create_standard_queue(sqs, 'emails',
    redrive_policy=sqs.redrive_policy('emails_dead', 3)
)
```

It means "move the message to the email_deadletters queue after four (3 + 1) failed attempts to send it to the recipient."

## Backoff policies

You can define the backoff policy for the entire environment or specific queue.

```python
queue = sqs.queue("emails", backoff_policy=DEFAULT_BACKOFF)

@queue.processor('send_email')
def send_email(to, subject, body):
    print(f"Sending email {subject} to {to}")
```

The default policy is the exponential backoff. We recommend setting both the backoff policy and the dead-letter queue to limit the maximum number of execution attempts.

Alternatively, you can set the backoff to IMMEDIATE_RETURN to re-execute the failed task immediately.

```python
queue = sqs.queue("emails", backoff_policy=IMMEDIATE_RETURN)

@queue.processor('send_email')
def send_email(to, subject, body):
    print(f"Sending email {subject} to {to}")
```

## Shutdown policies

When launching the queue processor with process_queue(), it's possible to optionally set when it has to be stopped.

Following shutdown policies are supported:

- IdleShutdown(idle_seconds): return from function when no new tasks are sent for a specific period.

- MaxTasksShutdown(max_tasks): return from function after processing at least max_task jobs. It can be helpful to prevent memory leaks.

The default policy is NeverShutdown. It's also possible to combine two previous policies with OrShutdown or AndShutdown policies or create custom classes for specific behavior.

Example of stopping processing the queue after 5 minutes of inactivity:

```python
from sqs_workers import SQSEnv
from sqs_workers.shutdown_policies import IdleShutdown

sqs = SQSEnv()
sqs.queue("foo").process_queue(shutdown_policy=IdleShutdown(idle_seconds=300))
```

## Processing a dead-letter queue by pushing back failed messages

The most common way to process a dead-letter queue is to fix the main bug causing messages to appear there in the first place and then to re-process these messages again.

With sqs-workers, in can be done by putting back all the messages from a dead-letter queue back to the main one. While processing the queue, the processor takes every message and pushes it back to the upstream queue with a hard-coded delay of 1 second.

Usage example:

    >>> from sqs_workers import JobQueue
    >>> from sqs_workers.shutdown_policies IdleShutdown
    >>> from sqs_workers.deadletter_queue import DeadLetterQueue
    >>> env = SQSEnv()
    >>> foo = env.queue("foo")
    >>> foo_dead = env.queue("foo_dead", DeadLetterQueue.maker(foo))
    >>> foo_dead.process_queue(shutdown_policy=IdleShutdown(10))

This code takes all the messages in the foo_dead queue and pushes them back to the foo queue. Then it waits 10 seconds to ensure no new messages appear, and quit.

## Processing a dead-letter queue by executing tasks in-place

Instead of pushing the tasks back to the main queue, you can execute them in place. For this, you need to copy the queue processors from the main queue to the deadletter.

Usage example:

    >>> env = SQSEnv()
    >>> foo = env.queue("foo")
    >>> foo_dead = env.queue("foo_dead")
    >>> foo.copy_processors(foo_dead)
    >>>
    >>> from sqs_workers.shutdown_policies IdleShutdown
    >>> foo_dead.process_queue(shutdown_policy=IdleShutdown(10))

This code takes all the messages in the foo_dead queue and executes them with processors from the "foo" queue. Then it waits 10 seconds to ensure no new messages appear, and quit.

## Codecs

Before being added to SQS, task parameters are encoded using a `Codec`. At the moment, 3 codecs are available:
- `pickle`: serialize with Pickle, using the default protocol version;
- `pickle_compat`: serialize with Pickle, using protocol version 2, which is compatible with Python 2;
- `json`: serialize with JSON.

By default, `pickle_compat` is used, for backward compatibility with previous versions of sqs-workers.

JSON is recommended if using untrusted data (see the notes about security in the [pickle docs](https://docs.python.org/3/library/pickle.html), or for compatibility with other systems. In all other cases, you should use `pickle` instead of `pickle_compat`, as it's more compact and efficient:

    >>> env = SQSEnv(codec="pickle")

## Using in unit tests with MemorySession

There is a special MemorySession, which can be used as a quick and dirty replacement for real queues in unit tests. If you have a function `create_task` which adds some tasks to the queue and you want to test how it works, you can technically write your tests like this:

```python
from sqs_workers import SQSEnv
env = SQSEnv()

def test_task_creation_side_effects():
    create_task()
    env.process_batch('foo')
    ...
```

The problem is that your test starts depending on AWS (or localstack) infrastructure, which you don't always need. What you can do instead is you can pass MemorySession to your SQSEnv instance.

```python
from sqs_workers import SQSEnv, MemorySession
env = SQSEnv(MemorySession())
```

Please note that MemorySession has some serious limitations, and may not fit well your use-case. Namely, when you work with MemorySession:

- Redrive policy doesn't work
- There is no differences between standard and FIFO queues
- FIFO queues don't support content-based deduplication
- Delayed tasks are executed ineffectively: the task is gotten from the queue, and if the time hasn't come, the task is put back.
- API can return slightly different results

## Contributing

Please see our guide [here](./CONTRIBUTING.md)

## Local Development

We use Poetry for dependency management & packaging.  Please see [here for setup instructions](https://python-poetry.org/docs/#installation).

Once you have Poetry installed, you can run the following to install the dependencies in a virtual environment:

```bash
poetry install
```

## Testing

We use pytest to run unittests, and tox to run them for all supported Python versions.

If you just run `pytest` or `tox`, all tests will be run against AWS, localstack, and MemorySession. You can disable those you don't want to use using the pytest `-k` flag, for instance using `-k localstack` or `-k 'not aws'`.

### Testing with AWS

Make sure you have your boto3 client configured ([ref](https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration)) and then run

```bash
poetry run pytest -k aws
```

Alternatively, to test all supported versions, run

```bash
poetry run tox -- -k aws
```

### Testing with localstack

Localstack tests should perform faster than testing against AWS, and besides, they work well in offline.

Run [ElasticMQ](https://github.com/softwaremill/elasticmq) and make sure that the SQS endpoint is available by the address localhost:4566:

```bash
docker run -p 4566:9324 --rm -it softwaremill/elasticmq-native
```

Then run

```bash
poetry run pytest -k localstack
```

or

```bash
poetry run tox -- -k localstack
```

### Testing with MemorySession

MemorySession should be even faster, but has all the limitations documented above. But it can still be useful to test some logic changes.

Simply run

```bash
poetry run pytest -k memory
```

or

```bash
poetry run tox -- -k memory
```

## Releasing new versions

- Bump version in `pyproject.toml`
- Update the CHANGELOG
- Commit the changes with a commit message "Version X.X.X"
- Tag the current commit with `vX.X.X`
- Create a new release on GitHub named `vX.X.X`
- GitHub Actions will publish the new version to PIP for you
