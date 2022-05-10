import importlib
import logging
from inspect import Signature
from typing import Any

logger = logging.getLogger(__name__)


def _bind_args(callback, args, kwargs, drop_extra):
    sig = Signature.from_callable(callback)
    bind_args = list(args)
    bind_kwargs = {ensure_string(k): v for k, v in kwargs.items()}

    if drop_extra:
        # Compatibility with Werkzeug's `drop_extra`: if true, we drop extra
        # positional and keyword arguments.
        # This implementation is not perfect, but should be good enough for most
        # real-world use cases.
        extra_args, extra_kwargs = [], {}

        # - Is there a *args argument?
        if not any(
            param.kind == param.VAR_POSITIONAL for param in sig.parameters.values()
        ):
            # No: drop extra positional arguments
            all_pos = [
                param.name
                for param in sig.parameters.values()
                if param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD)
            ]
            nb_pos = len(all_pos)
            bind_args, extra_args = bind_args[:nb_pos], bind_args[nb_pos:]

        # - Is there a **kwargs argument?
        if not any(
            param.kind == param.VAR_KEYWORD for param in sig.parameters.values()
        ):
            # No: drop extra keyword arguments
            all_kw = {
                param.name
                for param in sig.parameters.values()
                if param.kind in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY)
            }
            new_bind_kwargs = {}
            for key, value in bind_kwargs.items():
                if key in all_kw:
                    new_bind_kwargs[key] = value
                else:
                    extra_kwargs[key] = value
            bind_kwargs = new_bind_kwargs

        if extra_args or extra_kwargs:
            logger.debug(
                "Dropped %d extra positional arguments and %d extra keyword arguments",
                len(extra_args),
                len(extra_kwargs),
                extra={
                    "extra_args": extra_args,
                    "extra_kwargs": extra_kwargs,
                },
            )

    bound_args = sig.bind(*bind_args, **bind_kwargs)
    return bound_args


def validate_arguments(callback, args, kwargs, drop_extra=True):
    """Checks if the function accepts the provided arguments and keyword
    arguments. Returns a new `(args, kwargs)` tuple that can be passed to the
    function without causing a TypeError due to an incompatible signature.

    If the arguments are invalid, a TypeError is raised.

    Similar to Werkzeug's old `validate_arguments` function, but doesn't modify
    passed args and kwargs.
    """
    bound_args = _bind_args(callback, args, kwargs, drop_extra)
    return (bound_args.args, bound_args.kwargs)


def bind_arguments(callback, args, kwargs):
    """Bind the arguments provided into a dict, returning a dict of bound
    keyword arguments.

    Similar to Werkzeug's old `bind_arguments` function, but doesn't modify
    passed args and kwargs.
    """
    bound_args = _bind_args(callback, args, kwargs, False)
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
