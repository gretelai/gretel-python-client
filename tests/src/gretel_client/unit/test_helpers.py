from unittest.mock import patch

import pytest
import requests

from gretel_client import get_synthetics_config


_config = """
schema_version: 1.0

models:
  - synthetics:
      data_source: "__tmp__"
      params:
        epochs: 100
        batch_size: 1
        vocab_size: 0
        learning_rate: .001
        rnn_units: 64
        foo: null
"""


class RespOK:
    status_code = 200

    @property
    def content(self):
        return _config

class RespNotFound:
    status_code = 404

    @property
    def text(self):
        return "not found"


def test_success():
    with patch("requests.get") as mock_get:
        mock_get.return_value = RespOK()
        check = get_synthetics_config()
        _, args, _ = mock_get.mock_calls[0]
        assert args[0] == "https://raw.githubusercontent.com/gretelai/gretel-blueprints/main/config_templates/gretel/synthetics/default.yml"
        assert check == {'epochs': 100, 'batch_size': 1, 'vocab_size': 0, 'learning_rate': 0.001, 'rnn_units': 64}

def test_not_found():
    with patch("requests.get") as mock_get:
        mock_get.return_value = RespNotFound()
        with pytest.raises(RuntimeError) as err:
            get_synthetics_config()
        assert "404" in str(err)
        assert "not found" in str(err)