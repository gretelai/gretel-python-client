import os

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union

from gretel_client.gretel.exceptions import BaseConfigError, ConfigSettingError
from gretel_client.projects.exceptions import ModelConfigError
from gretel_client.projects.models import read_model_config


class ConfigDictName(str, Enum):
    """Name of the model parameter dict in the config."""

    ACTGAN = "actgan"
    AMPLIFY = "amplify"
    LSTM = "synthetics"
    TABULAR_DP = "tabular_dp"


@dataclass(frozen=True)
class ModelConfigSections:
    """Config sections for each model type."""

    model_name: str
    config_sections: List[str]


CONFIG_SETUP_DICT = {
    ConfigDictName.ACTGAN: ModelConfigSections(
        model_name="actgan",
        config_sections=["params", "generate", "privacy_filters", "evaluate"],
    ),
    ConfigDictName.AMPLIFY: ModelConfigSections(
        model_name="amplify",
        config_sections=["params", "evaluate"],
    ),
    ConfigDictName.LSTM: ModelConfigSections(
        model_name="lstm",
        config_sections=[
            "params",
            "generate",
            "validators",
            "fallback",
            "data_checks",
            "privacy_filters",
            "evaluate",
        ],
    ),
    ConfigDictName.TABULAR_DP: ModelConfigSections(
        model_name="tabular_dp",
        config_sections=["params", "generate"],
    ),
}


def create_model_config_from_base(
    base_config: Union[str, Path],
    job_label: Optional[str] = None,
    **non_default_settings,
) -> dict:
    """Create a Gretel model config by updating a base config.

    To update the base config, pass in keyword arguments, where the keys
    are config section names and the values are dicts of non-default settings.

    The base config can be given as a yaml file path or the name of one of
    the Gretel template files (without the extension) listed here:
    https://github.com/gretelai/gretel-blueprints/tree/main/config_templates/gretel/synthetics

    Example::

        # Create an ACTGAN config with 10 epochs.
        from gretel_client.gretel.config_setup import create_model_config_from_base
        config = create_model_config_from_base(
            base_config="tabular-actgan",
            params={"epochs": 10},
        )

    The model configs are documented at
    https://docs.gretel.ai/reference/synthetics/models. For ACTGAN, the
    available config sections are `params`, `generate`, and `privacy_filters`.

    Args:
        base_config: Base config name or yaml config path.
        job_label: Descriptive label to append to job the name.

    Raises:
        BaseConfigError: If the base config is an invalid name or path.
        ConfigSettingError: If the config section or setting format is invalid.

    Returns:
        The model config derived from the base template and non-default settings.
    """
    if not os.path.isfile(base_config):
        base_config = (
            base_config
            if base_config.startswith("synthetics/")
            else f"synthetics/{base_config}"
        )

    try:
        config = read_model_config(base_config)
    except ModelConfigError as e:
        raise BaseConfigError(
            f"`{base_config}` is not a valid base config. You must either "
            "pass a local yaml file path or use the name of a template yaml file "
            "(without the extension) listed here: https://github.com/gretelai/"
            "gretel-blueprints/tree/main/config_templates/gretel/synthetics"
        ) from e

    dict_name = list(config["models"][0].keys())[0]
    setup = CONFIG_SETUP_DICT[ConfigDictName(dict_name)]

    if job_label is not None:
        config["name"] = f"{config['name']}-{job_label}"

    for section, settings in non_default_settings.items():
        if section not in setup.config_sections:
            raise ConfigSettingError(
                f"`{section}` is not a valid `{setup.model_name}` config section. "
                f"Must be one of [{setup.config_sections}]."
            )
        if not isinstance(settings, dict):
            raise ConfigSettingError(
                f"Invalid value for the `{section}` keyword argument. "
                f"Must be a dict, but you gave `{type(settings)}`."
            )
        for k, v in settings.items():
            if section not in config["models"][0][dict_name]:
                config["models"][0][dict_name][section] = {}
            config["models"][0][dict_name][section][k] = v

    return config
