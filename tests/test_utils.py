from textwrap import TextWrapper

from sqs_workers.utils import (
    instantiate_from_dict,
    instantiate_from_string,
    string_to_object,
)


def test_string_to_object():
    splitext = string_to_object("os.path.splitext")
    assert splitext("foo.txt") == ("foo", ".txt")


def test_instantiate_from_dict():
    options = {"maker": "textwrap.TextWrapper", "width": 80}
    w = instantiate_from_dict(options)
    assert isinstance(w, TextWrapper)
    assert w.width == 80


def test_instantiate_from_string():
    w = instantiate_from_string("textwrap.TextWrapper", width=80)
    assert isinstance(w, TextWrapper)
    assert w.width == 80
