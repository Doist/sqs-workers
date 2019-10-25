import importlib
from typing import Any

from werkzeug.utils import bind_arguments, validate_arguments


def adv_validate_arguments(callback, args, kwargs):
    """
    "Advanced version" of Werkzeug's "validate_arguments" which doesn't modify
    passed args and kwargs

    :return: (args, kwargs) to pass to original function
    """
    bind_args = list(args)
    bind_kwargs = kwargs.copy()
    arguments, keyword_arguments = validate_arguments(callback, bind_args, bind_kwargs)
    return arguments, keyword_arguments


def adv_bind_arguments(callback, args, kwargs):
    """
    "Advanced version" of Werkzeug's "bind_arguments" which doesn't modify
    passed args and kwargs
    """
    bind_args = list(args)
    bind_kwargs = kwargs.copy()
    keyword_arguments = bind_arguments(callback, bind_args, bind_kwargs)
    return keyword_arguments


def string_to_object(string):
    # type: (str) -> Any
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
