import base64
import json
import pickle
import zlib


class JSONCodec(object):
    @staticmethod
    def serialize(job):
        return json.dumps(job)

    @staticmethod
    def deserialize(message_body):
        return json.loads(message_body)


class PickleCodec(object):
    @staticmethod
    def serialize(job):
        binary_data = pickle.dumps(job, protocol=pickle.HIGHEST_PROTOCOL)
        compressed_data = zlib.compress(binary_data)
        return base64.urlsafe_b64encode(compressed_data).decode('utf-8')

    @staticmethod
    def deserialize(message_body):
        compressed_data = base64.urlsafe_b64decode(message_body)
        binary_data = zlib.decompress(compressed_data)
        return pickle.loads(binary_data)


def get_codec(content_type):
    return CONTENT_TYPES_CODECS[content_type]


CONTENT_TYPES_CODECS = {
    'json': JSONCodec,
    'pickle': PickleCodec,
}
