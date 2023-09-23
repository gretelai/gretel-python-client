from pathlib import Path
from typing import Callable

import pytest

from gretel_client.gretel.config_setup import create_model_config_from_base
from gretel_client.gretel.exceptions import BaseConfigError, ConfigSettingError


@pytest.fixture
def config_file_path(get_fixture: Callable) -> Path:
    return get_fixture("tabular-actgan.yml")


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


def test_create_config_template_error():
    """Test exception from giving an invalid template."""
    with pytest.raises(BaseConfigError):
        create_model_config_from_base(base_config="invalid-template")


def test_create_config_settings_error():
    """Test exception from giving invalid settings."""
    with pytest.raises(ConfigSettingError):
        create_model_config_from_base(
            base_config="tabular-actgan",
            invalid_setting="invalid",
        )

    with pytest.raises(ConfigSettingError):
        create_model_config_from_base(
            base_config="tabular-actgan",
            params="must be a dict",
        )
