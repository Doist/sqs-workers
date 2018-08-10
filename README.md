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

Dead-letter queues and redrive
------------------------------

On creating the queue you can set the fallback dead-letter queue and redrive
policy, which can look like this

```python
from sqs_workers import SQSEnv
sqs = SQSEnv()
sqs.create_standard_queue('emails_deadletters')
sqs.create_standard_queue('emails', 
    redrive_policy=sqs.redrive_policy('emails_deadletters', 3)
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


Why it depends on werkzeug? 😱
------------------------------

The only reason is [werkzeug.utils.validate_arguments](http://werkzeug.pocoo.org/docs/dev/utils/#werkzeug.utils.validate_arguments)
which we love and we are lazy enough to move it to this codebase.

