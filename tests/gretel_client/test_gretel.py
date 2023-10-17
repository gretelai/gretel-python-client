from pathlib import Path
from typing import Callable

import pytest

from gretel_client.gretel.config_setup import create_model_config_from_base
from gretel_client.gretel.exceptions import BaseConfigError, ConfigSettingError


@pytest.fixture
def config_file_path(get_fixture: Callable) -> Path:
    return get_fixture("tabular-actgan.yml")


@pytest.fixture
def gpt_x_config_file_path(get_fixture: Callable) -> Path:
    return get_fixture("gpt_x_with_params_section.yml")


@pytest.fixture
def gpt_x_old_config_file_path(get_fixture: Callable) -> Path:
    return get_fixture("gpt_x_config_without_params.yml")


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


def test_create_config_template_error():
    """Test exception from giving an invalid template."""
    with pytest.raises(BaseConfigError):
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
