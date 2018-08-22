from werkzeug.utils import validate_arguments, bind_arguments


def adv_validate_arguments(callback, args, kwargs):
    """
    "Advanced version" of Werkzeug's "validate_arguments" which doesn't modify
    passed args and kwargs

    :return: (args, kwargs) to pass to original function
    """
    bind_args = list(args)
    bind_kwargs = kwargs.copy()
    arguments, keyword_arguments = validate_arguments(callback, bind_args,
                                                      bind_kwargs)
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
