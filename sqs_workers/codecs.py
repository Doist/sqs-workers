import base64
import json
import pickle
import zlib
from typing import Any, ClassVar, Dict, Protocol, Type

DEFAULT_CONTENT_TYPE = "pickle_compat"

# Highest version of the protocol, understood by Python2.
PICKLE_PY2_COMPAT_PROTO = 2


class Codec(Protocol):
    @classmethod
    def serialize(cls, message: Any) -> str:
        ...

    @classmethod
    def deserialize(cls, serialized: str) -> Any:
        ...


class JSONCodec:
    @classmethod
    def serialize(cls, message: Any) -> str:
        return json.dumps(message)

    @classmethod
    def deserialize(cls, serialized: str) -> Any:
        return json.loads(serialized)


class PickleCodec:
    protocol: ClassVar[int] = pickle.DEFAULT_PROTOCOL

    @classmethod
    def serialize(cls, message: Any) -> str:
        binary_data = pickle.dumps(message, protocol=cls.protocol)
        compressed_data = zlib.compress(binary_data)
        return base64.urlsafe_b64encode(compressed_data).decode("latin1")

    @classmethod
    def deserialize(cls, serialized: str) -> Any:
        compressed_data = base64.urlsafe_b64decode(serialized.encode("latin1"))
        binary_data = zlib.decompress(compressed_data)
        return pickle.loads(binary_data)


class PickleCompatCodec(PickleCodec):
    protocol = PICKLE_PY2_COMPAT_PROTO


def get_codec(content_type: str) -> Type[Codec]:
    return CONTENT_TYPES_CODECS[content_type]


CONTENT_TYPES_CODECS: Dict[str, Type[Codec]] = {
    "json": JSONCodec,
    "pickle": PickleCodec,
    "pickle_compat": PickleCompatCodec,
}
