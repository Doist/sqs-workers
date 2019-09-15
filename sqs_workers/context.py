from contextlib import contextmanager
from threading import local


class SQSContext(local):
    """
    Context which is implicitly passed to the worker via the job message. Can
    be used there for logging or profiling purposes, for example.

    Usage example.


        @sqs.processor('q1', 'hello_world', pass_context=True)
        def hello_world(username=None, context=None):
            print(f'Hello {username} from {context["remote_addr"]}')

        with sqs.context(remote_addr='127.0.0.1'):
            hello_world.delay(username='Foo')

        sqs.process_batch('q1')

    Alternatively, you can set the context like this.

        sqs.context['remote_addr'] = '127.0.0.1'
        hello_world.delay(username='Foo')

    And then, when the context needs to be cleared:

        sqs.context.clear()

    In a web application you usually call it at the end of the processing
    of the web request.
    """

    @classmethod
    def from_dict(cls, kwargs):
        """
        Create context from the dict
        """
        return cls(**kwargs)

    def __init__(self, **kwargs):
        """
        Create a new context and populate it with keys and values from kwargs
        """
        self._context = {}
        self.set(**kwargs)

    def set(self, **kwargs):
        """
        Clean up current context and replace it with values from kwargs
        """
        self._context = kwargs.copy()

    def update(self, **kwargs):
        """
        Extend current context with values from kwargs
        """
        self._context.update(kwargs)

    def clear(self):
        """
        Remove all values from the context
        """
        self._context.clear()

    def to_dict(self):
        """
        Convert context to dictionary
        """
        return self._context.copy()

    def get(self, key, default=None):
        """
        Get value by key from the context
        """
        return self._context.get(key, default)

    def __getitem__(self, item):
        """
        Dict API emulation. Get value by key from the context
        """
        return self._context[item]

    def __setitem__(self, key, value):
        """
        Dict API emulation. Set value by key
        """
        self._context[key] = value

    @contextmanager
    def __call__(self, **kwargs):
        """
        Run the context manager. Temporarily extend current context with
        kwargs until the end of the context manager. On exit from the
        with-statement restore back the original context.
        """
        orig_context = self._context.copy()
        try:
            self.update(**kwargs)
            yield self
        finally:
            self.set(**orig_context)

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self._context)
