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


def test_bind_arguments_with_var_keyword():
    """Test that bind_arguments correctly handles **kwargs.

    This test verifies the fix for issue #10, where the old werkzeug-based
    implementation would lose arguments when a function accepted **kwargs.
    """

    # Function with only **kwargs
    def func_kwargs_only(**kwargs):
        pass

    result = bind_arguments(func_kwargs_only, [], {"a": 1, "b": 2})
    assert result == {"kwargs": {"a": 1, "b": 2}}

    # Function with specific parameter and **kwargs
    def func_with_kwargs(a, **kwargs):
        pass

    result = bind_arguments(func_with_kwargs, [], {"a": 1, "b": 2})
    assert result == {"a": 1, "kwargs": {"b": 2}}

    # Decorator pattern mentioned in issue #10
    def decorator(*args, **kwargs):
        pass

    result = bind_arguments(decorator, [], {"a": 1})
    assert result == {"kwargs": {"a": 1}}

    # Function with *args and **kwargs
    def func_with_both(*args, **kwargs):
        pass

    result = bind_arguments(func_with_both, [1, 2], {"a": 3, "b": 4})
    assert result == {"args": (1, 2), "kwargs": {"a": 3, "b": 4}}
