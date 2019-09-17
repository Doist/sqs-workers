from typing import TYPE_CHECKING, Union

from sqs_workers import codecs
from sqs_workers.processor_mgr import ProcessorManagerProxy

if TYPE_CHECKING:
    from sqs_workers import MemoryEnv
    from sqs_workers import SQSEnv


class GenericQueue(ProcessorManagerProxy):
    def __init__(self, env, name):
        # type: (Union[SQSEnv, MemoryEnv], str) -> None
        self.env = env
        self.name = name

    def add_job(
        self,
        job_name,
        _content_type=codecs.DEFAULT_CONTENT_TYPE,
        _delay_seconds=None,
        _deduplication_id=None,
        _group_id=None,
        **job_kwargs
    ):
        """
        Add job to the queue. The body of the job will be converted to the text
        with one of the codes (by default it's "pickle")
        """
        codec = codecs.get_codec(_content_type)
        message_body = codec.serialize(job_kwargs)
        job_context = codec.serialize(self.env.context.to_dict())
        return self.add_raw_job(
            job_name,
            message_body,
            job_context,
            _content_type,
            _delay_seconds,
            _deduplication_id,
            _group_id,
        )
