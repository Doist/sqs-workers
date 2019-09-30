from sqs_workers.utils import string_to_object


def test_string_to_object():
    splitext = string_to_object("os.path.splitext")
    assert splitext("foo.txt") == ("foo", ".txt")
