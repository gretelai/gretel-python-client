from unittest.mock import Mock

import pytest

from gretel_client.navigator.workflow import DataDesignerWorkflow


def test_from_yaml():
    client = Mock()
    client.registry.return_value = [
        {"name": "my-name", "inputs": ["ok"], "output": "ok"}
    ]
    yaml_str = """
name: combine datasets
version: "2"
globals:
  seed: 1337
steps:
  - name: ids
    task: id_generator
    config: {}
"""
    workflow = DataDesignerWorkflow.from_yaml(client, yaml_str)
    assert workflow is not None
    assert workflow._globals == {
        "model_suite": "apache-2.0",
        "num_records": 10,
        "seed": 1337,
    }


def test_default_from_yaml():
    client = Mock()
    client.registry.return_value = [
        {"name": "my-name", "inputs": ["ok"], "output": "ok"}
    ]
    yaml_str = """
name: combine datasets
version: "2"
steps:
  - name: ids
    task: id_generator
    config: {}
"""
    with pytest.raises(TypeError):
        DataDesignerWorkflow.from_yaml(client, yaml_str)
