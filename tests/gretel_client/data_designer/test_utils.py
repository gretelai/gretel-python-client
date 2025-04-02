import json

from datetime import date
from unittest.mock import Mock, patch

from gretel_client.data_designer.utils import (
    CallbackOnMutateDict,
    camel_to_kebab,
    fetch_config_if_remote,
    get_task_log_emoji,
    make_date_obj_serializable,
)


def test_make_date_obj_serializable():
    obj = {"name": "test", "date": date(2023, 10, 5)}
    expected = {"name": "test", "date": "2023-10-05"}
    assert make_date_obj_serializable(obj) == expected


def test_make_date_obj_serializable_nested():
    obj = {
        "name": "test",
        "details": {"start_date": date(2023, 10, 5), "end_date": date(2023, 10, 6)},
    }
    expected = {
        "name": "test",
        "details": {"start_date": "2023-10-05", "end_date": "2023-10-06"},
    }
    assert make_date_obj_serializable(obj) == expected


def test_camel_to_kebab():
    assert camel_to_kebab("camelCaseString") == "camel-case-string"
    assert camel_to_kebab("anotherExampleString") == "another-example-string"
    assert camel_to_kebab("CamelCase") == "camel-case"
    assert camel_to_kebab("simple") == "simple"


def test_get_task_log_emoji():
    assert get_task_log_emoji("generate") == "🦜 "
    assert get_task_log_emoji("unknown_task") == ""


def test_fetch_config_if_remote():
    remote_url = (
        "https://raw.githubusercontent.com/gretelai/gretel-blueprints/main/config.json"
    )
    local_config = {"key": "value"}
    with patch("requests.get") as mock_get:
        mock_get.return_value.content.decode.return_value = json.dumps(local_config)
        assert fetch_config_if_remote(remote_url) == json.dumps(local_config)
        assert fetch_config_if_remote(local_config) == local_config


def test_callback_on_mutate_dict():
    mock_fn = Mock()
    mock_fn.return_value = None

    test_dict = CallbackOnMutateDict(mock_fn)
    test_dict["foo"] = "bar"
    assert mock_fn.call_count == 1
    mock_fn.reset_mock()

    test_dict = CallbackOnMutateDict(mock_fn)
    test_dict |= {"hi": "bye", "foo": "baz"}
    test_dict["foo"] = "bar"
    assert mock_fn.call_count == 2
    mock_fn.reset_mock()

    test_dict = CallbackOnMutateDict(mock_fn)
    test_dict |= {"hi": "bye", "foo": "baz"}
    test_dict.popitem()
    test_dict.popitem()
    assert mock_fn.call_count == 3
    mock_fn.reset_mock()

    test_dict = CallbackOnMutateDict(mock_fn)
    test_dict |= {"hi": "bye", "foo": "baz"}
    out = test_dict.pop("hi")
    assert mock_fn.call_count == 2
    assert out == "bye"
    mock_fn.reset_mock()

    test_dict = CallbackOnMutateDict(mock_fn)
    test_dict |= {"hi": "bye", "foo": "baz"}
    test_dict |= {"super": "hero"}
    assert mock_fn.call_count == 2
    assert test_dict == {"hi": "bye", "foo": "baz", "super": "hero"}
    mock_fn.reset_mock()

    test_dict = CallbackOnMutateDict(mock_fn)
    test_dict |= {"hi": "bye", "foo": "baz"}
    del test_dict["hi"]
    assert mock_fn.call_count == 2
    assert test_dict == {"foo": "baz"}
    mock_fn.reset_mock()

    test_dict = CallbackOnMutateDict(mock_fn)
    test_dict |= {"hi": "bye", "foo": "baz"}
    test_dict.update({"hi": "hello"})
    assert mock_fn.call_count == 2
    assert test_dict == {"hi": "hello", "foo": "baz"}
    mock_fn.reset_mock()
