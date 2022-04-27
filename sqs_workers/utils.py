import importlib
from inspect import Signature
from typing import Any


def _bind_args(callback, args, kwargs):
    sig = Signature.from_callable(callback)
    bind_args = list(args)
    bind_kwargs = {ensure_string(k): v for k, v in kwargs.items()}
    bound_args = sig.bind(*bind_args, **bind_kwargs)
    return bound_args


def validate_arguments(callback, args, kwargs):
    """Checks if the function accepts the provided arguments and keyword
    arguments. Returns a new `(args, kwargs)` tuple that can be passed to the
    function without causing a TypeError due to an incompatible signature.

    If the arguments are invalid, a TypeError is raised.

    Similar to Werkzeug's old `validate_arguments` function, but doesn't modify
    passed args and kwargs.
    """
    bound_args = _bind_args(callback, args, kwargs)
    return (bound_args.args, bound_args.kwargs)


def bind_arguments(callback, args, kwargs):
    """Bind the arguments provided into a dict, returning a dict of bound
    keyword arguments.

    Similar to Werkzeug's old `bind_arguments` function, but doesn't modify
    passed args and kwargs.
    """
    bound_args = _bind_args(callback, args, kwargs)
    return bound_args.arguments


def string_to_object(string: str) -> Any:
    """
    Convert full path string representation of the object to object itself.
    """
    chunks = string.rsplit(".", 1)
    if len(chunks) != 2:
        raise RuntimeError(
            "{} doesn't represent a full module path to object".format(string)
        )
    module_name, object_name = chunks
    mod = importlib.import_module(module_name)
    return getattr(mod, object_name)


def instantiate_from_string(string, **init_kwargs):
    """
    Create an object from a classname string and init kwargs.
    """
    class_ = string_to_object(string)
    return class_(**init_kwargs)


def instantiate_from_dict(options, maker_key="maker", **extra_init_kwargs):
    """
    Create an object from options with classname and init kwargs.
    """
    kwargs = options.copy()
    classname_value = kwargs.pop(maker_key)
    init_kwargs = dict(kwargs, **extra_init_kwargs)
    return string_to_object(classname_value)(**init_kwargs)


def ensure_string(obj: Any, encoding="utf-8", errors="strict") -> str:
    """Make sure an object is converted to a proper string representation."""
    if isinstance(obj, str):
        return obj
    elif isinstance(obj, bytes):
        return obj.decode(encoding, errors)
    else:
        return str(obj)
