import base64
import json
import pickle
import zlib

DEFAULT_CONTENT_TYPE = "pickle"


class JSONCodec(object):
    @staticmethod
    def serialize(message):
        return json.dumps(message)

    @staticmethod
    def deserialize(serialized):
        return json.loads(serialized)


class PickleCodec(object):
    @staticmethod
    def serialize(message):
        binary_data = pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
        compressed_data = zlib.compress(binary_data)
        return base64.urlsafe_b64encode(compressed_data).decode("latin1")

    @staticmethod
    def deserialize(serialized):
        compressed_data = base64.urlsafe_b64decode(serialized.encode("latin1"))
        binary_data = zlib.decompress(compressed_data)
        return pickle.loads(binary_data)


def get_codec(content_type):
    return CONTENT_TYPES_CODECS[content_type]


CONTENT_TYPES_CODECS = {"json": JSONCodec, "pickle": PickleCodec}
