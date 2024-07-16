from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock, patch

import pytest

from gretel_client.gretel.config_setup import (
    create_model_config_from_base,
    smart_load_yaml,
    smart_read_model_config,
)
from gretel_client.gretel.exceptions import (
    ConfigSettingError,
    InvalidYamlError,
    ModelConfigReadError,
)


@pytest.fixture
def config_file_path(get_fixture: Callable) -> Path:
    return get_fixture("gretel/tabular-actgan.yml")


@pytest.fixture
def gpt_x_config_file_path(get_fixture: Callable) -> Path:
    return get_fixture("gretel/gpt_x_with_params_section.yml")


@pytest.fixture
def gpt_x_old_config_file_path(get_fixture: Callable) -> Path:
    return get_fixture("gretel/gpt_x_config_without_params.yml")


def test_smart_load_yaml(config_file_path):
    """Test loading yaml configs using various input formats."""
    yaml_str = """
    base_config: tabular-actgan
    testing: true
    """
    assert smart_load_yaml("foo: bar") == {"foo": "bar"}
    assert smart_load_yaml({"foo": "bar"}) == {"foo": "bar"}
    assert isinstance(smart_load_yaml(yaml_str), dict)
    assert isinstance(smart_load_yaml(config_file_path), dict)
    assert isinstance(smart_load_yaml(str(config_file_path)), dict)


def test_smart_read_model_config(config_file_path):
    """Test reading model configs using various input formats."""
    yaml_str = """
    schema_version: 1
    name: testing
    models: 
      - actgan: 
          params: 
            epochs: 15
    """
    url = (
        "https://raw.githubusercontent.com/gretelai/gretel-blueprints/"
        "main/config_templates/gretel/synthetics/tabular-actgan.yml"
    )
    config_dict = dict(
        schema_version=1,
        name="testing",
        models=[{"actgan": {"params": {"epochs": 15}}}],
    )
    assert smart_read_model_config(config_dict) == config_dict
    assert smart_read_model_config(yaml_str) == config_dict
    assert isinstance(smart_read_model_config(config_file_path), dict)
    assert isinstance(smart_read_model_config(str(config_file_path)), dict)

    config_blueprint = smart_read_model_config(url)
    assert isinstance(config_blueprint, dict)
    assert config_blueprint == smart_read_model_config("tabular-actgan")
    assert config_blueprint == smart_read_model_config("synthetics/tabular-actgan")


def test_create_config_from_blueprint():
    """Test creating a Gretel model config from a synthetics blueprint."""
    settings = create_model_config_from_base(
        base_config="tabular-actgan",
        params={"epochs": 15},
        generate={"num_records": 100},
        privacy_filters={"similarity": None, "outliers": "medium"},
    )["models"][0]["actgan"]

    assert settings["params"]["epochs"] == 15
    assert settings["params"]["generator_dim"] == [1024, 1024]
    assert settings["generate"]["num_records"] == 100
    assert settings["privacy_filters"]["similarity"] is None
    assert settings["privacy_filters"]["outliers"] == "medium"


def test_create_config_from_file(config_file_path: Path):
    """Test creating a Gretel model config from a yaml file."""
    settings = create_model_config_from_base(
        base_config=str(config_file_path),
        params={"epochs": 10},
        privacy_filters={"similarity": "high", "outliers": None},
    )["models"][0]["actgan"]

    assert settings["params"]["epochs"] == 10
    assert settings["params"]["generator_dim"] == [128, 128]
    assert settings["generate"]["num_records"] == 5000
    assert settings["privacy_filters"]["similarity"] == "high"
    assert settings["privacy_filters"]["outliers"] is None


def test_update_non_nested_setting(gpt_x_config_file_path: Path):
    """Test updating a non-nested config setting."""
    settings = create_model_config_from_base(
        base_config=str(gpt_x_config_file_path),
        pretrained_model="gretelai/we-are-awesome",
        column_name="custom_name",
    )["models"][0]["gpt_x"]

    assert settings["pretrained_model"] == "gretelai/we-are-awesome"
    assert settings["column_name"] == "custom_name"


def test_no_validation_on_dev():
    # Additional keys are accepted for internal testing on dev.
    with patch(
        "gretel_client.gretel.config_setup.get_session_config"
    ) as mock_get_session_config:
        mock_session_config = MagicMock()
        mock_session_config.stage = "dev"
        mock_get_session_config.return_value = mock_session_config

        settings = create_model_config_from_base(
            base_config="navigator-ft",
            is_gretel_dev=True,
            params={"rope_scaling_factor": 2},
            extra_stuff={"foo": "bar"},
        )["models"][0]["navigator_ft"]

        assert settings["params"]["rope_scaling_factor"] == 2
        assert settings["extra_stuff"]["foo"] == "bar"


def test_gpt_x_backwards_compatibility(
    gpt_x_old_config_file_path: Path, gpt_x_config_file_path: Path
):
    """Test config to old format if base is old format and user passes `params`."""

    # old format
    settings = create_model_config_from_base(
        base_config=str(gpt_x_old_config_file_path),
        params={"learning_rate": 0.1, "batch_size": 16},
    )["models"][0]["gpt_x"]

    assert "params" not in settings
    assert settings["batch_size"] == 16
    assert settings["learning_rate"] == 0.1

    # new format
    settings = create_model_config_from_base(
        base_config=str(gpt_x_config_file_path),
        params={"learning_rate": 0.1, "batch_size": 16},
    )["models"][0]["gpt_x"]

    assert "params" in settings
    assert settings["params"]["batch_size"] == 16
    assert settings["params"]["learning_rate"] == 0.1


def test_invalid_yaml_string():
    """Test exception from giving an invalid yaml string."""
    with pytest.raises(InvalidYamlError, match="Loaded yaml must be a dict."):
        smart_load_yaml("invalid-yaml")

    with pytest.raises(InvalidYamlError, match="invalid yaml config format"):
        smart_load_yaml(42)


def test_model_config_read_error():
    """Test exception from giving an invalid model config."""
    invalid_config = """
    name: config-with-missing-field
    models: null
    """
    with pytest.raises(ModelConfigReadError, match="not a valid string for the input"):
        smart_read_model_config("invalid-blueprint-name")

    with pytest.raises(ModelConfigReadError, match="not a valid input config format"):
        smart_read_model_config([42])

    with pytest.raises(ModelConfigReadError, match="not a valid Gretel model config"):
        smart_read_model_config(invalid_config)


def test_create_config_template_error():
    """Test exception from giving an invalid template."""
    with pytest.raises(ModelConfigReadError):
        create_model_config_from_base(base_config="invalid-template")


def test_create_config_settings_error():
    """Test exception from giving invalid settings."""
    with pytest.raises(ConfigSettingError):
        create_model_config_from_base(
            base_config="tabular-actgan",
            invalid_non_nested_setting="invalid",
        )

    with pytest.raises(ConfigSettingError):
        create_model_config_from_base(
            base_config="tabular-actgan",
            invalid_section={"invalid": "section"},
        )

    with pytest.raises(ConfigSettingError):
        create_model_config_from_base(
            base_config="navigator-ft",
            extra_stuff={"foo": "bar"},
        )
