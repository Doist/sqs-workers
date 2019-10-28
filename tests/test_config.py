import pytest

from sqs_workers.config import Config


@pytest.fixture
def parent():
    return Config()


@pytest.fixture
def child(parent):
    return Config(parent=parent)


def test_set_get(parent, child):
    child["foo"] = "bar"
    assert child["foo"] == "bar"


def test_inheritance(parent, child):
    parent["foo"] = "bar"
    assert child["foo"] == "bar"


def test_inheritance_overwrite(parent, child):
    parent["foo"] = "bar"
    child["foo"] = "baz"
    assert child["foo"] == "baz"


def test_key_error(parent, child):
    with pytest.raises(KeyError):
        child["foo"]


def test_get_default(child):
    assert child.get("foo", "bar") == "bar"


def test_get_object(child):
    child["foo"] = "posixpath.join"
    assert child.get_object("foo")("a", "b") == "a/b"


def test_make_child(parent, child):
    parent["foo"] = "bar"
    grandson = child.make_child()
    assert grandson["foo"] == "bar"
