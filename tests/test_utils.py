from textwrap import TextWrapper

import pytest

from sqs_workers.utils import (
    bind_arguments,
    instantiate_from_dict,
    instantiate_from_string,
    string_to_object,
    validate_arguments,
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


def test_bind_arguments_converts_to_unicode():
    def foo(a, b):
        pass

    kwargs = bind_arguments(foo, [], {b"a": 1, b"b": 2})
    assert kwargs == {"a": 1, "b": 2}


def test_validate_arguments_converts_to_unicode():
    def foo(a, b):
        pass

    args, kwargs = validate_arguments(foo, [], {b"a": 1, b"b": 2})
    assert args == (1, 2)
    assert kwargs == {}


def test_validate_arguments_drops_extra():
    def foo(a, /, b, c, *, d):
        pass

    # Nothing dropped
    args, kwargs = validate_arguments(foo, (1, 2), {"c": 3, "d": 4})
    assert args == (1, 2, 3)
    assert kwargs == {"d": 4}

    # Extra positional argument dropped
    args, kwargs = validate_arguments(foo, (1, 2, 3, 4), {"d": 5})
    assert args == (1, 2, 3)
    assert kwargs == {"d": 5}

    # Extra keyword arguments dropped
    args, kwargs = validate_arguments(
        foo, (1,), {"a": -1, "b": 2, "c": 3, "foo": 4, "d": 5, "e": 6}
    )
    assert args == (1, 2, 3)
    assert kwargs == {"d": 5}


def test_bind_arguments_raises_on_extra():
    def foo(a, /, b, c, *, d):
        pass

    # No error
    args = bind_arguments(foo, (1, 2), {"c": 3, "d": 4})
    assert args == {"a": 1, "b": 2, "c": 3, "d": 4}

    # Too many positional arguments
    with pytest.raises(TypeError):
        bind_arguments(foo, (1, 2, 3, 4), {"d": 5})

    # Unexpected keyword arguments
    with pytest.raises(TypeError):
        bind_arguments(foo, (1,), {"a": -1, "b": 2, "c": 3, "d": 4})
    with pytest.raises(TypeError):
        bind_arguments(
            foo,
            (1, 2),
            {
                "b": -1,
                "c": 3,
                "d": 4,
            },
        )
    with pytest.raises(TypeError):
        bind_arguments(
            foo,
            (1, 2),
            {
                "foo": 42,
                "c": 3,
                "d": 4,
            },
        )
