from abc import ABC, abstractmethod


class BatchingConfiguration(ABC):
    """
    Abstract class defining the contract for configuring whether
    a processor receives messages 1 by 1, or processes them in batch.
    """

    @property
    @abstractmethod
    def batching_enabled(self) -> bool:
        """If false, process messages 1 by 1, otherwise process in batch"""
        ...

    @property
    @abstractmethod
    def batch_size(self) -> int:
        """Number of messages to process at once if batching_enabled"""
        ...


class NoBatching(BatchingConfiguration):
    """Configures the processor to process each message 1 by 1"""

    @property
    def batching_enabled(self) -> bool:
        return False

    @property
    def batch_size(self) -> int:
        return 1


class BatchMessages(BatchingConfiguration):
    """Configures the processor to process the messages in batches"""

    def __init__(self, number_of_messages):
        self.number_of_messages = number_of_messages

    @property
    def batching_enabled(self) -> bool:
        return True

    @property
    def batch_size(self) -> int:
        return self.number_of_messages
