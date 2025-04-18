from dataclasses import dataclass, field
from typing import Any, Optional

from sqs_workers.utils import (
    instantiate_from_dict,
    instantiate_from_string,
    string_to_object,
)


@dataclass(frozen=True)
class Config:
    """Config object with hierarchy support."""

    parent: Optional["Config"] = field(repr=False, default=None)
    options: dict[str, Any] = field(default_factory=dict)
    maker_key: str = "maker"

    def __setitem__(self, key: str, value):
        self.options.__setitem__(key, value)

    def __getitem__(self, item):
        if item in self.options:
            return self.options[item]
        if self.parent:
            return self.parent[item]
        raise KeyError(f"{item} is undefined")

    def get(self, item, default=None):
        try:
            return self[item]
        except KeyError:
            return default

    def get_object(self, item):
        """Get an object (usually a class) from the config."""
        value = self[item]
        return string_to_object(value)

    def get_instance(self, item, **kwargs):
        """Get an instances form the config and optional set of kwargs."""
        value = self[item]
        if isinstance(value, dict):
            return instantiate_from_dict(value, maker_key=self.maker_key, **kwargs)
        return instantiate_from_string(value, **kwargs)

    def make_child(self, options=None):
        if options is None:
            options = {}
        return Config(parent=self, options=options)
