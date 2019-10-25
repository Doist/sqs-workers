from typing import Any, Dict, Optional

import attr

from sqs_workers.utils import (
    instantiate_from_dict,
    instantiate_from_string,
    string_to_object,
)


@attr.s(frozen=True)
class Config(object):
    """
    Config object with hierarchy support.
    """

    parent = attr.ib(repr=False, default=None)  # type: Optional["Config"]
    options = attr.ib(factory=dict)  # type: Dict[str, Any]
    maker_key = attr.ib(default="maker")

    def __setitem__(self, key, value):
        self.options.__setitem__(key, value)

    def __getitem__(self, item):
        if item in self.options:
            return self.options[item]
        if self.parent:
            return self.parent[item]
        raise KeyError("{0} is undefined".format(item))

    def get(self, item, default=None):
        try:
            return self[item]
        except KeyError:
            return default

    def get_object(self, item):
        """
        Get an object (usually a class) from the config.
        """
        value = self[item]
        return string_to_object(value)

    def get_instance(self, item, **kwargs):
        """
        Get an instances form the config and optional set of kwargs.
        """
        value = self[item]
        if isinstance(value, dict):
            return instantiate_from_dict(value, maker_key=self.maker_key, **kwargs)
        else:
            return instantiate_from_string(value, **kwargs)

    def make_child(self, options=None):
        if options is None:
            options = {}
        return Config(parent=self, options=options)
