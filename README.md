SQS Workers
===========

How can I use it?
-----------------

Unless you are the part of the [Doist development team](https://github.com/orgs/Doist/people), 
you most likely don't need it. It's something opinionated, built out of our own internal needs
and probably provides little value for outside developers.

Queue processors are in abundance (see http://queues.io/ for examples), and
there is no shortage of SQS queue processors on
[PyPI](https://pypi.org/search/?q=SQS), so please don't put your high hopes
on this particular implementation

Got it, but how can I start using it anyway?
--------------------------------------------

Install the package with

```bash
pip install sqs-workers
```

Configure your boto3 library to provide access requisites for your installation
with [something like this](https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration):

```bash
aws configure
```

Don't forget to set your preferred AWS region.

Then you will start managing two systems (most likely, from the same codebase):
one of them adds messages to the queue and another one executes them.

```python
from sqs_workers import SQSEnv

# This environment will use AWS requisites from ~/.aws/ directory
sqs = SQSEnv()

# Create a new queue.
# Note that you can use AWS web interface for the same action as well, the
# web interface provides more options. You only need to do it once.
sqs.create_standard_queue('emails')

# Register a queue processor
@sqs.processor('emails', 'send_email')
def send_email(to, subject, body):
    print(f"Sending email {subject} to {to}")
```


Then there are two ways of adding tasks to the queue. Classic (aka "explicit"):

```python
sqs.add_job(
    'emails', 'send_email', to='user@examile.com', subject='Hello world', body='hello world')
```


And the "Celery way" (we mimic the Celery API to some extent)

```python
send_email.delay(to='user@examile.com', subject='Hello world', body='hello world')
```

To process the queue you have to run workers manually. Create a new file which
will contain the definition of the sqs object and register all processors (most likely,
by importing necessary modules from your project), and then run SQS

```python
from sqs_workers import SQSEnv
sqs = SQSEnv()
...
sqs.process_queue('emails')
```

In production we usually don't handle multiple queues in the same process,
but for the development environment it's easier to tackle with all the queues
at once with

```python
sqs.process_queues()
```

Serialization
-------------

There are two serializers: json and pickle.


FIFO queues
-----------

Fifo queues can be created with `create_fifo_queue` and has to have the name
which ends with ".fifo". The dead-letter queue has to have a name
`something_dead.fifo`.

```python
from sqs_workers import SQSEnv
sqs = SQSEnv()
sqs.create_fifo_queue('emails_dead.fifo')
sqs.create_fifo_queue('emails.fifo', 
    redrive_policy=sqs.redrive_policy('emails_dead.fifo', 3)
)
```

Unless the flag `content_based_deduplication` is set, every message has to be
sent with an attribute `_deduplication_id`. By default all messages have the
same message group `default`, but you can change it with `_group_id`.

```python
sqs.add_job(
    'emails.fifo', 'send_email', _deduplication_id=subject, _group_id=email, **kwargs)
```

[More about FIFO queues on AWS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html)


Exception processing
--------------------

If task processing ended up with an exception, the error is logged and the
task is returned back to the queue after a while. The exact behavior is defined
by queue settings.

Batch processing
----------------

Instead of using `sqs.processor` decorator you can use `sqs.batch_processor`.
In this case the function must accept parameter "messages" containing
the list of dicts.

Custom processors
-----------------

You can define your own processor or batch processor if you need to perform
some specific actions before of after executing a specific task.

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

Working with contexts
---------------------

Context which is implicitly passed to the worker via the job message. Can
be used there for logging or profiling purposes, for example.

Usage example.


```python
@sqs.processor('q1', 'hello_world', pass_context=True)
def hello_world(username=None, context=None):
    print(f'Hello {username} from {context["remote_addr"]}')

with sqs.context(remote_addr='127.0.0.1'):
    hello_world.delay(username='Foo')

sqs.process_batch('q1')
```

Alternatively, you can set the context like this.

```python
sqs.context['remote_addr'] = '127.0.0.1'
hello_world.delay(username='Foo')
```

And then, when the context needs to be cleared:

```python
sqs.context.clear()
```

In a web application you usually call it at the end of the processing
of the web request.


Automatic applying of the context for all tasks
------------------------------------------------

Instead of dealing with the context inside every processing function, you
can perform this in processors by subclassing them.

```python
import os
from sqs_workers import SQSEnv
from sqs_workers.processors import BatchProcessor, Processor

class CustomProcessor(Processor):
    def process(self, job_kwargs, job_context):
        os.environ['REMOTE_ADDR'] = job_context['remote_addr']
        super(CustomProcessor, self).process(job_kwargs, job_context)
        os.unsetenv('REMOTE_ADDR')


class CustomBatchProcessor(BatchProcessor):
    def process(self, jobs, context):
        # in this case context variable contains the list of
        # context objects, and it may or may not make sense to
        # process them before starting the main function.
        print("Jobs context", context)
        super(CustomBatchProcessor, self).process(jobs, context)


sqs = SQSEnv(
    processor_maker=CustomProcessor,
    batch_processor_maker=CustomBatchProcessor,
)
```
 

Dead-letter queues and redrive
------------------------------

On creating the queue you can set the fallback dead-letter queue and redrive
policy, which can look like this

```python
from sqs_workers import SQSEnv
sqs = SQSEnv()
sqs.create_standard_queue('emails_dead')
sqs.create_standard_queue('emails', 
    redrive_policy=sqs.redrive_policy('emails_dead', 3)
)
```

This means "move the message to the email_deadletters queue after four (3 + 1)
failed attempts to send it to the recipient"


Backoff policies
----------------

You can define the backoff policy for the entire environment or for specific
processor. 


```python
@sqs.processor('emails', 'send_email', backoff_policy=DEFAULT_BACKOFF)
def send_email(to, subject, body):
    print(f"Sending email {subject} to {to}")
```

Default policy is the exponential backoff. It's recommended to always set
both backoff policy and dead-letter queue to limit the maximum number
of execution attempts.

Alternatively you can set the backoff to IMMEDIATE_RETURN to re-execute
failed task immediately.

```python
@sqs.processor('emails', 'send_email', backoff_policy=IMMEDIATE_RETURN)
def send_email(to, subject, body):
    print(f"Sending email {subject} to {to}")
```

Shutdown policies
-----------------

When launching the queue processor with process_queue(), it's possible
to optionally set when it has to be stopped.

Following shutdown policies are supported:

- IdleShutdown(idle_seconds): return from function when no new tasks
  are seend for specific period of time

- MaxTasksShutdown(max_tasks): return from function after processing at
  least max_task jobs. Can be helpful to prevent memory leaks

Default policy is NeverShutdown. It's also possible to combine two previous 
policies with OrShutdown or AndShutdown policies, or create
custom classes for specific behavior.

Example of stopping processing the queue after 5 minutes of inactivity:

```python
from sqs_workers import SQSEnv
from sqs_workers.shutdown_policies import IdleShutdown

sqs = SQSEnv()
sqs.process_queue('foo', shutdown_policy=IdleShutdown(idle_seconds=300))
```

Processing dead-letter queue by pushing back failed messages
------------------------------------------------------------

The most common way to process a dead-letter queue is to fix the main bug
causing messages to appear there in the first place, and then to re-process
these messages again.

With sqs-workers in can be done by putting back all the messages from a
dead-letter queue back to the main one. It can be performed with a special
fallback processor called DeadLetterProcessor.

The dead-letter processor has opinion on how queues are organized and uses some
hard-coded options.

It is supposed to process queues "something_dead" which is supposed to be
a configured dead-letter queue for "something". While processing the queue,
the processor takes every message and push it back to the queue "something"
with a hard-coded delay of 1 second.

If the queue name does't end with "_dead", the DeadLetterProcessor behaves
like generic FallbackProcessor: shows the error message and keep message in
the queue. It's made to prevent from creating infinite loops when the
message from the dead letter queue is pushed back to the same queue, then
immediately processed by the same processor again, etc.

Usage example:

```python
from sqs_workers import SQSEnv
from sqs_workers.processors import DeadLetterProcessor
from sqs_workers.shutdown_policies import IdleShutdown

sqs = SQSEnv(fallback_processor_maker=DeadLetterProcessor)
sqs.process_queue("foo_dead", shutdown_policy=IdleShutdown(10))
```

This code takes all the messages in foo_dead queue and push them back to
the foo queue. Then it waits 10 seconds to ensure no new messages appear,
and quit.


Processing dead-letter with processors from the main queue
----------------------------------------------------------

Instead of pushing back tasks to the main queue you can copy processors
from the main queue to dead-letter and process all tasks in place.

Usage example:

```python
from sqs_workers import SQSEnv
from sqs_workers.shutdown_policies import IdleShutdown

sqs = SQSEnv()
...
sqs.copy_processors('foo', 'foo_dead')
sqs.process_queue('foo_dead', shutdown_policy=IdleShutdown(10))
```


Using in unit tests with MemoryEnv
----------------------------------

There is a special MemoryEnv which can be used as a quick'n'dirty replacement
for real queues in unit tests. If you have a function `create_task` which adds
some tasks to the queue and you want to test how it works, you can technically 
write your tests like this:

```python
from sqs_workers import SQSEnv
env = SQSEnv()

def test_task_creation_side_effects():
    create_task()
    env.process_batch('foo')
    ...
```

The problem is that your test starts depending on AWS (or localstack)
infrastructure, which you don't always need. What you can do instead is you
can replace SQSEnv with a MemoryEnv() and rewrite your tests like this.

```python
from sqs_workers.memory_env import MemoryEnv
env = MemoryEnv()
```

Please note that MemoryEnv has some serious limitations, and may not fit well
your use-case. Namely, when you work with MemoryEnv:

- Redrive policy doesn't work
- There is no differences between standard and FIFO queues
- FIFO queues don't support content-based deduplication
- Delayed tasks executed ineffectively: the task is gotten from the queue,
  and if the time hasn't come yet, the task is put back.
- API can return slightly different results


Testing with AWS
----------------

Make sure you have all dependencies installed, and boto3 client configured
([ref](https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration))
and then run

```bash
pytest -k aws
```

Alternatively, to test all supported versions, run

```bash
tox -- -k aws
```

Testing with localstack
-----------------------

Localstack tests should perform faster than testing against AWS, and besides,
they work well in offline.

Run [localstack](https://github.com/localstack/localstack) and make sure
that the SQS endpoint is available by its default address http://localhost:4576

Then run

```bash
pytest -k localstack
```

or

```bash
tox -- -k localstack
```


Why it depends on werkzeug? 😱
------------------------------

The only reason is [werkzeug.utils.validate_arguments](http://werkzeug.pocoo.org/docs/dev/utils/#werkzeug.utils.validate_arguments)
which we love and we are lazy enough to move it to this codebase.

