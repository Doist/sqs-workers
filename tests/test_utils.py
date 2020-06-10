from textwrap import TextWrapper

from sqs_workers.utils import (
    adv_bind_arguments,
    adv_validate_arguments,
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


def test_adv_bind_arguments_converts_to_unicode():
    def foo(a, b):
        pass

    kwargs = adv_bind_arguments(foo, [], {b"a": 1, b"b": 2})
    assert kwargs == {"a": 1, "b": 2}


def test_adv_validate_arguments_converts_to_unicode():
    def foo(a, b):
        pass

    args, kwargs = adv_validate_arguments(foo, [], {b"a": 1, b"b": 2})
    assert args == (1, 2)
    assert kwargs == {}
