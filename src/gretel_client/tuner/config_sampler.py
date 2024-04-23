import json

from collections import defaultdict
from copy import copy, deepcopy
from enum import Enum
from numbers import Number
from pathlib import Path
from typing import Any, Callable, Optional, Union

from optuna.trial import Trial

from gretel_client.gretel.config_setup import (
    CONFIG_SETUP_DICT,
    create_model_config_from_base,
    extract_model_config_section,
    smart_load_yaml,
)
from gretel_client.tuner.exceptions import (
    InvalidSamplerConfigError,
    InvalidSampleTypeError,
    SamplerCallbackError,
)


class _TunerEnum(str, Enum):
    @classmethod
    def values(cls):
        return [n.value for n in cls]

    def __str__(self):
        return self.value


class TunableSection(_TunerEnum):
    """Possible sections of the model config that can be tuned."""

    # nested sections
    GENERATE = "generate"
    PARAMS = "params"
    PRIVACY_FILTERS = "privacy_filters"

    # GPT-X top-level sections (i.e., params that are not nested)
    PRETRAINED_MODEL = "pretrained_model"
    PROMPT_TEMPLATE = "prompt_template"
    COLUMN_NAME = "column_name"

    # DGAN top-level sections
    FEATURE_COLUMNS = "feature_columns"

    def is_top_level(self):
        return self not in [self.GENERATE, self.PARAMS, self.PRIVACY_FILTERS]


class SampleType(_TunerEnum):
    """Possible sampling methods for model config parameters."""

    FIXED = "fixed"
    CHOICES = "choices"
    INT_RANGE = "int_range"
    FLOAT_RANGE = "float_range"
    LOG_RANGE = "log_range"

    def _check_list_like(self, value, *, required_type=None):
        if not isinstance(value, (list, tuple)):
            raise InvalidSampleTypeError(
                f"{self.upper()} must be a list. You gave '{value}'."
            )
        if required_type is not None and not all(
            isinstance(v, required_type) for v in value
        ):
            raise InvalidSampleTypeError(
                f"{self.upper()} values must be of type {required_type}. You gave '{value}'"
            )

    def _check_range_like(self, value, *, step_optional):
        if not step_optional:
            if len(value) != 2:
                raise InvalidSampleTypeError(
                    f"{self.upper()} must have exactly 2 elements. "
                    f"You gave '{value}'."
                )
        else:
            if (len(value) < 2) or (len(value) > 3):
                raise InvalidSampleTypeError(
                    f"{self.upper()} must have exactly 2 or 3 elements. "
                    f"You gave '{value}'."
                )
            if len(value) == 3:
                if value[2] <= 0:
                    raise InvalidSampleTypeError(
                        f"The third element of {self.upper()} is the step size "
                        f"and must be > 0. You gave '{value}'."
                    )
                if len(value) == 3 and value[2] > value[1] - value[0]:
                    raise InvalidSampleTypeError(
                        f"The third element of {self.upper()} is the step size "
                        f"and must be less than the range. You gave '{value}'. "
                        "Consider a smaller step size or using CHOICES instead."
                    )
        if value[1] < value[0]:
            raise InvalidSampleTypeError(
                f"{self.upper()} must have min < max. You gave '{value}'."
            )

    def check_type_of_sampling(self, sampling):
        """Check that the type of the given value is valid for the sample type."""
        msg = f"You gave '{sampling}'."
        if self == self.CHOICES:
            self._check_list_like(sampling)
        elif self == self.INT_RANGE:
            self._check_list_like(sampling, required_type=int)
            self._check_range_like(sampling, step_optional=True)
        elif self == self.FLOAT_RANGE:
            self._check_list_like(sampling, required_type=Number)
            self._check_range_like(sampling, step_optional=True)
        elif self == self.LOG_RANGE:
            self._check_list_like(sampling, required_type=Number)
            if not all(s > 0 for s in sampling):
                raise InvalidSampleTypeError(
                    f"{self.upper()} values must be > 0. {msg}"
                )
            self._check_range_like(sampling, step_optional=False)

    @classmethod
    def validate(cls, value):
        if value not in cls.values():
            raise InvalidSampleTypeError(
                f"'{value}' is not a valid sample type. "
                f"Valid options are {cls.values()}."
            )


class ModelConfigSampler:
    """A class for sampling model config parameters for tuning with Optuna.

    In general, we recommend using the `gretel_client.gretel.Gretel` interface
    for hyperparameter tuning, in which case you do not need to use this class.
    However, you may need to use this class directly for highly-specific use cases.

    Args:
        sampler_config: Config as a yaml file path, yaml string, or dict.
        sampler_callback: Callback function that is applied to the model section of
            the config after it is sampled. This is useful for applying custom
            logic/constraints to the sampled config. The callback function must
            accept a model config section as input and return an updated config.
        **kwargs: Keyword arguments to override the given sampler config.
            Keywords must follow the nested structure of the input yaml file.
            See example below.

    Raises:
        InvalidSamplerConfigError: If the given config and/or kwargs format is invalid.
        InvalidSampleTypeError: If a sample type or its value(s) are invalid.

    Example::

        from gretel_client.tuner.config_sampler import ModelConfigSampler

        config_str = '''
            base_config: "tabular-actgan"
            params:
                epochs:
                    fixed: 50
                batch_size:
                    choices: [500, 1000]
        '''
        sampler = ModelConfigSampler(
            config_str,
            params={"generator_lr": {"log_range": [0.001, 0.01]}}
        )
    """

    def __init__(
        self,
        sampler_config: Union[str, Path, dict],
        *,
        sampler_callback: Optional[Callable] = None,
        **kwargs,
    ):
        sampler_config = smart_load_yaml(sampler_config)

        if "base_config" not in sampler_config:
            raise InvalidSamplerConfigError(
                "You must define `base_config` in the sampler config."
            )

        self._config = deepcopy(sampler_config)
        self._choices_mapping = {}

        # "metric" may be in the config, since it is used for the tuner
        self._config.pop("metric", None)

        self.base_config = create_model_config_from_base(
            self._config.pop("base_config")
        )

        self.model_type, self.base_model_config_section = extract_model_config_section(
            self.base_config
        )

        self._setup = CONFIG_SETUP_DICT[self.model_type]

        self._callback = sampler_callback
        self.tunable_sections = [
            TunableSection(s)
            for s in (self._setup.config_sections + self._setup.extra_kwargs)
            if s in TunableSection.values()
        ]

        self._update_sample_config(**kwargs)

    def _update_sample_config(self, **kwargs):
        for section, settings in kwargs.items():
            if not isinstance(settings, dict):
                raise InvalidSamplerConfigError(
                    "Values of kwargs must be a dict. "
                    f"You gave '{settings}' for '{section}'."
                )
            for key, value in settings.items():
                if isinstance(value, dict):
                    sample_type, sampling = next(iter(value.items()))
                    # first remove any existing sampling for the given param
                    self._config.setdefault(section, {}).pop(key, None)
                    self._config[section].setdefault(key, {})[sample_type] = sampling
                else:
                    # first remove any existing sampling for the given top-level param
                    self._config.pop(section, None)
                    self._config.setdefault(section, {})[key] = value

        self._validate_sampler_config()

    def _validate_sampler_config(self):
        for section, settings in self._config.items():
            if section not in self.tunable_sections:
                raise InvalidSamplerConfigError(
                    f"'{section}' is not a valid tuner section/param for "
                    f"{self._setup.model_name.upper()}. Valid options are "
                    f"{[s.value for s in self.tunable_sections]}."
                )
            if not isinstance(settings, dict):
                raise InvalidSamplerConfigError(
                    f"The value of '{section}' must be a dict. You gave "
                    f"'{settings}' for '{section}'."
                )
            if TunableSection(section).is_top_level():
                if len(settings) != 1:
                    raise InvalidSamplerConfigError(
                        "Only one sample type can be defined for each param. You gave "
                        f"'{settings}' for the top-level config param '{section}'."
                    )
                sample_type, sampling = next(iter(settings.items()))
                SampleType.validate(sample_type)
                SampleType(sample_type).check_type_of_sampling(sampling)
            else:
                for param, sample_settings in settings.items():
                    if not isinstance(sample_settings, dict):
                        raise InvalidSamplerConfigError(
                            f"The value of '{param}' must be a dict. You gave "
                            f"'{sample_settings}' for '{param}' in section '{section}'."
                        )
                    if len(sample_settings) != 1:
                        raise InvalidSamplerConfigError(
                            "Only one sample type can be defined per parameter. "
                            f"You gave '{sample_settings}' for '{param}' in section '{section}'."
                        )
                    sample_type, sampling = next(iter(sample_settings.items()))
                    SampleType.validate(sample_type)
                    SampleType(sample_type).check_type_of_sampling(sampling)

    def _map_choices(self, name, sampling):
        """Map the names of choices to their original values.

        This mapping allows Optuna to sample choices from a list
        of lists/tuples, which is not supported natively.

        Args:
            name: The name of the parameter.
            sampling: The list of choices.
        """
        if len(sampling) > 0 and isinstance(sampling[0], (list, tuple)):
            mapping = {str(s): s for s in sampling}
        else:
            mapping = {s: s for s in sampling}
        self._choices_mapping[name] = mapping

    def _get_trial_sample(
        self,
        trial: Trial,
        sample_type: SampleType,
        name: str,
        sampling: Any,
    ) -> Any:
        """Get a trial sample for the given parameter."""
        if sample_type == SampleType.CHOICES:
            self._map_choices(name, sampling)
            sample = trial.suggest_categorical(
                name, choices=list(self._choices_mapping[name].keys())
            )
            sample = self._choices_mapping[name][sample]
        elif sample_type == SampleType.INT_RANGE:
            step = sampling[2] if len(sampling) == 3 else 1
            sample = trial.suggest_int(name, *sampling[:2], step=step)
        elif sample_type == SampleType.FLOAT_RANGE:
            step = sampling[2] if len(sampling) == 3 else None
            sample = trial.suggest_float(name, *sampling[:2], step=step)
        elif sample_type == SampleType.LOG_RANGE:
            sample = trial.suggest_float(name, *sampling, log=True)
        elif sample_type == SampleType.FIXED:
            sample = sampling
        else:
            raise InvalidSampleTypeError(
                f"{sample_type.upper()} is not a valid sample type. "
                f"Valid options are {SampleType.values()}."
            )
        return sample

    def callback(self, model_config_section: dict) -> dict:
        if self._callback is None:
            return model_config_section
        elif not callable(self._callback):
            raise SamplerCallbackError(
                "The callback function must be callable. "
                f"You gave '{self._callback}'."
            )
        else:
            updated_config = self._callback(model_config_section)
            if not isinstance(updated_config, dict):
                raise SamplerCallbackError(
                    "The callback function must return a dict. "
                    f"You gave '{updated_config}'."
                )
            # Copy the values of the updated config in case the callback set
            # them to references within the original config. This was added
            # after observing pointer-like syntax when writing to yaml files.
            for k1, v1 in updated_config.items():
                if isinstance(v1, dict):
                    for k2, v2 in v1.items():
                        updated_config[k1][k2] = copy(v2)
                else:
                    updated_config[k1] = copy(v1)
            return updated_config

    def create_trial_config(self, trial: Trial) -> dict:
        """Create a model config for the given trial."""
        config = deepcopy(self.base_config)
        model_config = deepcopy(self.base_model_config_section)

        for section, settings in self._config.items():
            if TunableSection(section).is_top_level():
                sample_type, sampling = next(iter(settings.items()))
                model_config[section] = self._get_trial_sample(
                    trial=trial,
                    sample_type=sample_type,
                    name=section,
                    sampling=sampling,
                )
            else:
                for param, sample_settings in settings.items():
                    sample_type, sampling = next(iter(sample_settings.items()))
                    model_config[section][param] = self._get_trial_sample(
                        trial=trial,
                        sample_type=sample_type,
                        name=f"{section}:{param}",
                        sampling=sampling,
                    )

        config["models"][0][self.model_type] = self.callback(model_config)

        return config

    def parse_trial_params(self, trial_params: dict) -> dict:
        """Parse the trial params into expected model config params."""
        parsed = defaultdict(dict)
        for tuner_param, sample in trial_params.items():
            if tuner_param in self._choices_mapping:
                sample = self._choices_mapping[tuner_param][sample]
            if ":" in tuner_param:
                section, param = tuner_param.split(":")
                parsed[section][param] = sample
            else:
                parsed[tuner_param] = sample
        return dict(parsed)

    def convert_trial_params_to_config(self, trial_params: dict) -> dict:
        """Convert the given Optuna trial params to a Gretel config."""
        model_config = deepcopy(self.base_model_config_section)

        # update model config with sampled params
        for section, sample in self.parse_trial_params(trial_params).items():
            if TunableSection(section).is_top_level():
                model_config[section] = sample
            else:
                for param, param_sample in sample.items():
                    model_config.setdefault(section, {})[param] = param_sample

        # update model config with fixed params
        for section, settings in self._config.items():
            if value := settings.get(SampleType.FIXED.value):
                if TunableSection(section).is_top_level():
                    model_config[section] = value
                else:
                    for param, param_value in value.items():
                        model_config.setdefault(section, {})[param] = param_value

        config = deepcopy(self.base_config)
        config["models"][0][self.model_type] = self.callback(model_config)

        return config

    def __repr__(self):
        return json.dumps(self._config, indent=4)
