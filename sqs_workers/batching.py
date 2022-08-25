from abc import ABC, abstractmethod


class BatchingConfiguration(ABC):
    """
    Defining the contract for configuring whether a processor sends
    messages 1 by 1 to a call handler, or as a list of messages.
    """

    @property
    @abstractmethod
    def batching_enabled(self) -> bool:
        """
        If false, messages are sent 1 by 1 to the call handler
        If true, messages are sent as a list to the call handler
        """
        ...

    @property
    @abstractmethod
    def batch_size(self) -> int:
        """
        Number of messages to process at once if batching_enabled
        """
        ...


class NoBatching(BatchingConfiguration):
    """Configures the processor to send messages 1 by 1 to the call handler"""

    @property
    def batching_enabled(self) -> bool:
        return False

    @property
    def batch_size(self) -> int:
        return 1


class BatchMessages(BatchingConfiguration):
    """Configures the processor to send a list of messages to the call handler"""

    def __init__(self, batch_size):
        self.number_of_messages = batch_size

    @property
    def batching_enabled(self) -> bool:
        return True

    @property
    def batch_size(self) -> int:
        return self.number_of_messages
