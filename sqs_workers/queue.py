from typing import TYPE_CHECKING, Union

from sqs_workers.processor_mgr import ProcessorManagerProxy

if TYPE_CHECKING:
    from sqs_workers import MemoryEnv
    from sqs_workers import SQSEnv


class GenericQueue(ProcessorManagerProxy):
    def __init__(self, env, name):
        # type: (Union[SQSEnv, MemoryEnv], str) -> None
        self.env = env
        self.name = name
