"""
Helper functions to create and delete queues on SQS.
"""


def create_standard_queue(
    env,
    queue_name,
    message_retention_period=None,
    visibility_timeout=None,
    redrive_policy=None,
):
    """
    Create a new standard queue
    """
    attrs = {}
    kwargs = {"QueueName": env.get_sqs_queue_name(queue_name), "Attributes": attrs}
    if message_retention_period is not None:
        attrs["MessageRetentionPeriod"] = str(message_retention_period)
    if visibility_timeout is not None:
        attrs["VisibilityTimeout"] = str(visibility_timeout)
    if redrive_policy is not None:
        attrs["RedrivePolicy"] = redrive_policy.__json__()
    ret = env.sqs_client.create_queue(**kwargs)
    return ret["QueueUrl"]


def create_fifo_queue(
    env,
    queue_name,
    content_based_deduplication=False,
    message_retention_period=None,
    visibility_timeout=None,
    redrive_policy=None,
):
    """
    Create a new FIFO queue. Note that queue name has to end with ".fifo"

    - "content_based_deduplication" turns on automatic content-based
      deduplication of messages in the queue

    - redrive_policy can be None or an object, generated with
      redrive_policy() method of SQS. In the latter case if defines the
      way failed messages are processed.
    """
    attrs = {"FifoQueue": "true"}
    kwargs = {"QueueName": env.get_sqs_queue_name(queue_name), "Attributes": attrs}
    if content_based_deduplication:
        attrs["ContentBasedDeduplication"] = "true"
    if message_retention_period is not None:
        attrs["MessageRetentionPeriod"] = str(message_retention_period)
    if visibility_timeout is not None:
        attrs["VisibilityTimeout"] = str(visibility_timeout)
    if redrive_policy is not None:
        attrs["RedrivePolicy"] = redrive_policy.__json__()
    ret = env.sqs_client.create_queue(**kwargs)
    return ret["QueueUrl"]


def delete_queue(env, queue_name):
    """
    Delete the queue
    """
    env.sqs_resource.get_queue_by_name(
        QueueName=env.get_sqs_queue_name(queue_name)
    ).delete()
