import json

from datetime import date
from unittest.mock import patch

from gretel_client.data_designer.types import CodeLang
from gretel_client.data_designer.utils import (
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
