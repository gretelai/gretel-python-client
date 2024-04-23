import math

from pathlib import Path
from typing import Callable

import optuna
import pytest

from gretel_client.gretel.config_setup import extract_model_config_section
from gretel_client.tuner.config_sampler import (
    ModelConfigSampler,
    SampleType,
    TunableSection,
)
from gretel_client.tuner.exceptions import (
    InvalidSamplerConfigError,
    InvalidSampleTypeError,
    ModelMetricMismatchError,
)
from gretel_client.tuner.metrics import GretelMetricName, GretelQualityScore
from gretel_client.tuner.tuner import GretelTuner


def _check_value(value, sample_type, sampling):
    if sample_type == SampleType.FIXED:
        assert value == sampling
    elif sample_type == SampleType.CHOICES:
        assert value in sampling
    elif sample_type == SampleType.LOG_RANGE:
        assert value >= sampling[0] and value <= sampling[1]
    elif sample_type in [SampleType.INT_RANGE, SampleType.FLOAT_RANGE]:
        assert value >= sampling[0] and value <= sampling[1]
        if len(sampling) == 3:
            num_steps = (value - sampling[0]) / sampling[2]
            assert math.isclose(num_steps, round(num_steps, 0), abs_tol=1e-8)
    else:
        raise ValueError(f"Unknown sample type {sample_type}")


@pytest.mark.parametrize(
    "tuner_config",
    ["tuner/tuner_config_tabular.yml", "tuner/tuner_config_natural_language.yml"],
)
def test_config_sampler(tuner_config: Path, get_fixture: Callable):
    """Test that the config sampler generates params within the given constraints."""
    sampler = ModelConfigSampler(get_fixture(tuner_config))

    study = optuna.create_study(direction="maximize")
    trial = optuna.trial.Trial(study, study._storage.create_new_trial(study._study_id))
    trial_config = sampler.create_trial_config(trial)
    _, model_config_section = extract_model_config_section(trial_config)

    for section, settings in sampler._config.items():
        assert section in TunableSection.values()
        if TunableSection(section).is_top_level():
            sample_type, sampling = next(iter(settings.items()))
            assert sample_type in SampleType.values()
            _check_value(
                value=model_config_section[section],
                sample_type=sample_type,
                sampling=sampling,
            )
        else:
            for param, sample_settings in settings.items():
                sample_type, sampling = next(iter(sample_settings.items()))
                assert sample_type in SampleType.values()
                _check_value(
                    value=model_config_section[section][param],
                    sample_type=sample_type,
                    sampling=sampling,
                )

    optuna.delete_study(study.study_name, study._storage)


def test_sampler_callback_constraints():
    dim = [518, 518, 518, 518]
    config_str = f"""
    base_config: tabular-actgan
    metric: gretel_sqs
    params:
        generator_dim: 
            fixed: {dim}
    """

    def callback(c):
        c["params"]["discriminator_dim"] = c["params"]["generator_dim"]
        return c

    sampler = ModelConfigSampler(config_str, sampler_callback=callback)
    study = optuna.create_study(direction="maximize")
    trial = optuna.trial.Trial(study, study._storage.create_new_trial(study._study_id))
    trial_config = sampler.create_trial_config(trial)
    _, c = extract_model_config_section(trial_config)
    assert c["params"]["discriminator_dim"] == dim


def test_override_config_settings_via_kwargs():
    config_str = """
    base_config: "natural-language"
    column_name:
        fixed: intent_and_text
    pretrained_model:
        choices: ["gretelai/mpt-7b", "meta-llama/Llama-2-7b-chat-hf"]
    params:
        epochs: 
            fixed: null
        batch_size: 
            choices: [8, 16]
        learning_rate: 
            log_range: [0.001, 0.015]
    """
    kwargs = dict(
        pretrained_model={"fixed": "meta-llama/Llama-2-7b-chat-hf"},
        params={
            "batch_size": {"fixed": 100},
            "epochs": {"choices": [100, 1000]},
            "warmup_steps": {"int_range": [10, 100]},
            "learning_rate": {"log_range": [0.001, 0.01]},
        },
        generate={
            "num_records": {"fixed": 100},
            "temperature": {"float_range": [0.1, 1.5]},
        },
    )
    sampler = ModelConfigSampler(sampler_config=config_str, **kwargs)
    for key, value in kwargs.items():
        assert sampler._config[key] == value


def test_invalid_sampler_config_missing_base_config():
    config_str = """
    metric: gretel_sqs
    params:
        batch_size: 
            fixed: 500
        epochs: 
            choices: [1, 2, 3]
    """
    with pytest.raises(InvalidSamplerConfigError, match="must define `base_config`"):
        ModelConfigSampler(config_str)


def test_invalid_sampler_more_than_one_param_sample_type():
    config_str = """
    base_config: tabular-actgan
    params:
        batch_size: 
            fixed: 500
        epochs: 
            choices: [1, 2, 3]
            int_range: [10, 100]
    """
    with pytest.raises(
        InvalidSamplerConfigError, match="one sample type can be defined per parameter"
    ):
        ModelConfigSampler(config_str)


def test_invalid_sampler_config_invalid_section_name():
    config_str = """
    base_config: tabular-actgan
    params:
        batch_size: 
            fixed: 500
        epochs:
            choices: [1, 2, 4]
    invalid_section:
        param: 
            fixed: 500
    """
    with pytest.raises(InvalidSamplerConfigError, match="not a valid tuner section"):
        ModelConfigSampler(config_str)


def test_invalid_sample_type():
    config_str = """
    base_config: tabular-actgan
    params:
        batch_size: 
            fixed: 500
        epochs: 
            choices: [1, 2, 3]
        param: 
            invalid_sample_type: 500
    """
    with pytest.raises(InvalidSampleTypeError, match="not a valid sample type"):
        ModelConfigSampler(config_str)


def test_invalid_choices_sample_type():
    config_str = """
    base_config: tabular-actgan
    params:
        batch_size: 
            fixed: 500
        epochs: 
            choices: 10
    """
    with pytest.raises(InvalidSampleTypeError, match="must be a list"):
        ModelConfigSampler(config_str)


def test_invalid_int_range_sample_type():
    config_str = """
    base_config: tabular-actgan
    params:
        batch_size: 
            fixed: 500
        epochs: 
            int_range: [10.0, 100]
    """
    with pytest.raises(InvalidSampleTypeError, match="values must be of type"):
        ModelConfigSampler(config_str)


def test_invalid_float_range_sample_type():
    config_str = """
    base_config: tabular-actgan
    params:
        batch_size: 
            fixed: 500
        generator_lr: 
            float_range: ["nope", 0.01]
    """
    with pytest.raises(InvalidSampleTypeError, match="values must be of type"):
        ModelConfigSampler(config_str)


def test_invalid_log_range_sample_type():
    config_str = """
    base_config: tabular-actgan
    params:
        batch_size: 
            fixed: 500
        generator_lr: 
            log_range: [-1.0, 10.0]
    """
    with pytest.raises(InvalidSampleTypeError, match="values must be > 0"):
        ModelConfigSampler(config_str)


def test_invalid_log_range_no_optional_step():
    config_str = """
    base_config: tabular-actgan
    params:
        generator_lr: 
            log_range: [0.01, 0.1, 0.01]
    """
    with pytest.raises(InvalidSampleTypeError, match="must have exactly 2 elements"):
        ModelConfigSampler(config_str)


def test_invalid_range_length():
    config_str = """
    base_config: tabular-actgan
    params:
        batch_size: 
            fixed: 500
        epochs: 
            int_range: [10, 100, 1000, 10000]
    """
    with pytest.raises(
        InvalidSampleTypeError, match="must have exactly 2 or 3 elements"
    ):
        ModelConfigSampler(config_str)


def test_invalid_range_min_max_order():
    config_str = """
    base_config: tabular-actgan
    params:
        generator_lr: 
            float_range: [100.0, 10.0]
    """
    with pytest.raises(InvalidSampleTypeError, match="must have min < max"):
        ModelConfigSampler(config_str)


def test_invalid_log_range_min_max_order():
    config_str = """
    base_config: tabular-actgan
    params:
        generator_lr: 
            log_range: [0.1, 0.001]
    """
    with pytest.raises(InvalidSampleTypeError, match="must have min < max"):
        ModelConfigSampler(config_str)


def test_invalid_range_step_too_big():
    config_str = """
    base_config: tabular-actgan
    params:
        epochs: 
            int_range: [10, 100, 1000]
    """
    with pytest.raises(InvalidSampleTypeError, match="must be less than the range"):
        ModelConfigSampler(config_str)


@pytest.mark.parametrize(
    "model_type,metric_name",
    [
        ("tabular-actgan", GretelMetricName.TEXT_SEMANTIC),
        ("natural-language", GretelMetricName.FCS),
    ],
)
def test_tuner_model_metric_mismatch(model_type: str, metric_name: GretelMetricName):
    with pytest.raises(ModelMetricMismatchError):
        GretelTuner(
            config_sampler=ModelConfigSampler(f"""base_config: {model_type}"""),
            metric=GretelQualityScore(metric_name),
        )
