import os
import uuid

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Optional, Tuple, Union

import yaml

from gretel_client.config import get_session_config
from gretel_client.gretel.artifact_fetching import ReportType
from gretel_client.gretel.exceptions import (
    ConfigSettingError,
    InvalidYamlError,
    ModelConfigReadError,
)
from gretel_client.projects.exceptions import ModelConfigError
from gretel_client.projects.models import read_model_config

SYNTHETICS_BLUEPRINT_REPO = (
    "https://github.com/gretelai/gretel-blueprints/"
    "tree/main/config_templates/gretel/synthetics"
)


# Default parameters for the Navigator and Natural Language inference APIs.
@dataclass(frozen=True)
class NavigatorDefaultParams:
    temperature: float = 0.7
    top_k: int = 40
    top_p: float = 0.95


@dataclass(frozen=True)
class NaturalLanguageDefaultParams:
    temperature: float = 0.6
    top_k: int = 43
    top_p: float = 0.9
    max_tokens: int = 512


class ModelType(str, Enum):
    """Name of the model parameter dict in the config.

    Note: The values are the names used in the model configs.
    """

    # tabular
    ACTGAN = "actgan"
    AMPLIFY = "amplify"
    LSTM = "synthetics"
    NAVIGATOR_FT = "navigator_ft"
    TABULAR_DP = "tabular_dp"

    # text
    GPT_X = "gpt_x"

    # time series
    DGAN = "timeseries_dgan"


@dataclass(frozen=True)
class ModelConfigSections:
    """Config sections for each model type.

    Args:
        model_name: Model name used in the url of the model docs.
        config_sections: List of nested config sections (e.g., `params`).
        data_source_optional: If True, the `data_source` config parameter is optional.
        report_type: The type of quality report generated by the model.
        extra_kwargs: List of non-nested config sections.
    """

    model_name: str
    config_sections: List[str]
    data_source_optional: bool
    report_type: Optional[ReportType] = None
    extra_kwargs: Optional[List[str]] = None


CONFIG_SETUP_DICT = {
    ModelType.ACTGAN: ModelConfigSections(
        model_name="actgan",
        config_sections=["params", "generate", "privacy_filters", "evaluate"],
        data_source_optional=False,
        report_type=ReportType.SQS,
        extra_kwargs=["ref_data"],
    ),
    ModelType.AMPLIFY: ModelConfigSections(
        model_name="amplify",
        config_sections=["params", "evaluate"],
        data_source_optional=False,
        report_type=ReportType.SQS,
        extra_kwargs=["ref_data"],
    ),
    ModelType.LSTM: ModelConfigSections(
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
        data_source_optional=False,
        report_type=ReportType.SQS,
        extra_kwargs=["ref_data"],
    ),
    ModelType.NAVIGATOR_FT: ModelConfigSections(
        model_name="navigator_fine_tuning",
        config_sections=["params", "generate", "evaluate"],
        data_source_optional=False,
        report_type=ReportType.SQS,
        extra_kwargs=[
            "group_training_examples_by",
            "order_training_examples_by",
            "ref_data",
        ],
    ),
    ModelType.TABULAR_DP: ModelConfigSections(
        model_name="tabular_dp",
        config_sections=["params", "generate", "evaluate"],
        data_source_optional=False,
        report_type=ReportType.SQS,
        extra_kwargs=["ref_data"],
    ),
    ModelType.GPT_X: ModelConfigSections(
        model_name="gpt",
        config_sections=["params", "peft_params", "privacy_params", "generate"],
        data_source_optional=True,
        report_type=ReportType.SQS,
        extra_kwargs=[
            "pretrained_model",
            "prompt_template",
            "column_name",
            "validation",
            "ref_data",
        ],
    ),
    ModelType.DGAN: ModelConfigSections(
        model_name="dgan",
        config_sections=["params", "generate"],
        data_source_optional=False,
        report_type=None,
        extra_kwargs=[
            "attribute_columns",
            "df_style",
            "discrete_columns",
            "example_id_column",
            "feature_columns",
            "ref_data",
            "time_column",
        ],
    ),
}


def _backwards_compat_transform_config(
    config: dict, non_default_settings: dict
) -> dict:
    """
    If the base config is in old format *and* the user passes in a params dict, move the
    non-default params to the base (non-nested) config level to be consistent with old format.
    """
    model_type, model_config_section = extract_model_config_section(config)
    if (
        model_type == ModelType.GPT_X.value
        and "params" in non_default_settings
        and "params" not in model_config_section
    ):
        params = non_default_settings.pop("params")
        model_config_section.update(params)
    return config


def create_model_config_from_base(
    base_config: Union[str, Path, dict],
    job_label: Optional[str] = None,
    **non_default_settings,
) -> dict:
    """Create a Gretel model config by updating a base config.

    To update the base config, pass in keyword arguments, where the keys
    are config section names and the values are dicts of non-default settings.
    If the parameter is not nested within a section, pass it directly as
    a keyword argument.

    The base config can be given as a dict, yaml file path, yaml string, or
    the name of one of the base config files (without the extension) listed here:
    https://github.com/gretelai/gretel-blueprints/tree/main/config_templates/gretel/synthetics

    Args:
        base_config: Base config name, yaml path, yaml string, or dict.
        job_label: Descriptive label to append to job the name.

    Raises:
        ModelConfigReadError: If the input base config format is not valid.
        ConfigSettingError: If a config section or setting format is invalid.

    Returns:
        The model config derived from the base template and non-default settings.

    Examples::

        from gretel_client.gretel.config_setup import create_model_config_from_base

        # Create an ACTGAN config with 10 epochs.
        config = create_model_config_from_base(
            base_config="tabular-actgan",
            params={"epochs": 10},
        )

        # Create a GPT config with a custom column name and 100 epochs.
        config = create_model_config_from_base(
            base_config="natural-language",
            column_name="custom_name",  # not nested in a config section
            params={"epochs": 100},  # nested in the `params` section
        )
    """
    config = smart_read_model_config(base_config)
    model_type, model_config_section = extract_model_config_section(config)
    setup = CONFIG_SETUP_DICT[ModelType(model_type)]
    is_gretel_dev = get_session_config().stage == "dev"

    config = _backwards_compat_transform_config(config, non_default_settings)

    if job_label is not None:
        config["name"] = f"{config['name']}-{job_label}"

    for section, settings in non_default_settings.items():
        if not isinstance(settings, dict):
            extra_kwargs = setup.extra_kwargs or []
            if section in extra_kwargs or is_gretel_dev:
                model_config_section[section] = settings
            else:
                raise ConfigSettingError(
                    f"`{section}` is an invalid keyword argument. Valid options "
                    f"include {setup.config_sections + extra_kwargs}."
                )
        elif section in setup.config_sections or is_gretel_dev:
            model_config_section.setdefault(section, {}).update(settings)
        else:
            raise ConfigSettingError(
                f"`{section}` is not a valid `{setup.model_name}` config section. "
                f"Must be one of [{setup.config_sections}]."
            )
    return config


def extract_model_config_section(config: Union[str, Path, dict]) -> Tuple[str, dict]:
    """Extract the model type and config dict from a Gretel model config.

    Args:
        config: The Gretel config name, path, or dict.

    Returns:
        A tuple of the model type and the model section from the config.
    """
    return next(iter(smart_read_model_config(config)["models"][0].items()))


def get_model_docs_url(model_type: str) -> str:
    """Get the URL for the model docs.

    Args:
        model_type: The model keyword in the model config.

    Returns:
        The URL for the model docs.
    """
    model_name = CONFIG_SETUP_DICT[model_type].model_name.replace("_", "-")  # type: ignore
    return f"https://docs.gretel.ai/create-synthetic-data/models/synthetics/gretel-{model_name}"


def smart_load_yaml(yaml_in: Union[str, Path, dict]) -> dict:
    """Return the yaml config as a dict given flexible input types.

    Args:
        config: The config as a dict, yaml string, or yaml file path.

    Raises:
        InvalidYamlError: If the input yaml format is not valid.

    Returns:
        The config as a dict.
    """
    if isinstance(yaml_in, dict):
        yaml_out = yaml_in
    elif isinstance(yaml_in, Path) or (
        isinstance(yaml_in, str) and os.path.isfile(yaml_in)
    ):
        with open(yaml_in) as file:
            yaml_out = yaml.safe_load(file)
    elif isinstance(yaml_in, str):
        yaml_out = yaml.safe_load(yaml_in)
    else:
        raise InvalidYamlError(
            f"'{yaml_in}' is an invalid yaml config format. "
            "Valid options are: dict, yaml string, or yaml file path."
        )

    if not isinstance(yaml_out, dict):
        raise InvalidYamlError(
            f"Loaded yaml must be a dict. Got {yaml_out}, "
            f"which is of type {type(yaml_out)}."
        )

    return yaml_out


def smart_read_model_config(
    config_in: Union[str, Path, dict], config_name_prefix: Optional[str] = None
) -> dict:
    """Read a Gretel model config from a dict, yaml file, or yaml string.

    Args:
        config_in: The config as a dict, yaml string, or yaml file path.
        config_name_prefix: If the config does not have a `name` attribute. One
            will automatically be created using the prefix with a UUID slug at the end.

    Raises:
        ModelConfigReadError: If the input config format is not valid.

    Returns:
        The config as a dict.
    """
    if isinstance(config_in, dict):
        config_out = config_in
    elif isinstance(config_in, Path):
        config_out = smart_load_yaml(config_in)
    elif isinstance(config_in, str):
        if config_in.startswith("https://"):
            config_out = read_model_config(config_in)
        elif (
            ":" not in config_in
            and "\n" not in config_in
            and not os.path.isfile(config_in)
        ):
            try:
                config_out = read_model_config(
                    config_in
                    if (
                        config_in.startswith("synthetics/")
                        or config_in.startswith("transform/")
                        or config_in.startswith("evaluate/")
                    )
                    else f"synthetics/{config_in}"
                )
            except ModelConfigError as e:
                raise ModelConfigReadError(
                    f"'{config_in}' is not a valid string for the input config. Valid "
                    "strings are: a yaml path, a yaml string, or the name of a template "
                    "yaml config (without the extension) listed "
                    f"here: {SYNTHETICS_BLUEPRINT_REPO}."
                ) from e
        else:
            config_out = smart_load_yaml(config_in)
    else:
        raise ModelConfigReadError(
            f"'{config_in}' is not a valid input config format. Valid inputs are: "
            "a Gretel config dict, a yaml config path, a yaml string, or the "
            "name of a template yaml config (without the extension) listed "
            f"here: {SYNTHETICS_BLUEPRINT_REPO}."
        )

    if config_name_prefix is not None and "name" not in config_out:
        config_name_prefix = config_name_prefix.removesuffix("-")
        config_out["name"] = f"{config_name_prefix}-{uuid.uuid4().hex[:6]}"

    if any([p not in config_out for p in ["schema_version", "models", "name"]]):
        raise ModelConfigReadError(
            f"The given config '{config_out}' is not a valid Gretel model config."
        )

    return config_out
