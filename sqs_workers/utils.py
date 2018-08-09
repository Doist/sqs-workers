from werkzeug.utils import validate_arguments


def adv_validate_arguments(callback, args, kwargs):
    """
    "Advanced version" of Werkzeug's "validate_arguments".

    - Knows how to handle object methods
    - Doesn't modify passed args and kwargs

    :return: (args, kwargs) to pass to original function
    """
    bind_args = list(args)
    bind_kwargs = kwargs.copy()
    if hasattr(callback, 'im_self'):
        bind_args.insert(0, callback.im_self)
    arguments, keyword_arguments = validate_arguments(callback, bind_args,
                                                      bind_kwargs)
    if hasattr(callback, 'im_self'):
        arguments = arguments[1:]
    return arguments, keyword_arguments
