import pytest

from sqs_workers.codecs import JSONCodec, PickleCodec, PickleCompatCodec


@pytest.mark.parametrize("codec", [PickleCodec, PickleCompatCodec, JSONCodec])
def test_encode_decode(codec):
    foo = {"message": "hello world"}
    foo_str = codec.serialize(foo)
    same_foo = codec.deserialize(foo_str)
    assert foo == same_foo
