from pathlib import Path
from typing import Callable

import pytest

from gretel_client.projects import Project
from gretel_client.tuner import (
    ACTGANConfigSampler,
    BaseConfigSampler,
    GretelHyperParameterTuner,
)
from gretel_client.tuner.metrics import SDMetricsScore


@pytest.fixture
def data_source(get_fixture: Callable) -> Path:
    return get_fixture("account-balances.csv")


@pytest.mark.parametrize("Sampler", [ACTGANConfigSampler])
def test_tuner(project: Project, data_source: Path, Sampler: BaseConfigSampler):
    """Test each tuner from end to end using the Gretel SQS metric."""
    tuner = GretelHyperParameterTuner(Sampler())
    results = tuner.run(data_source=data_source, project=project, n_trials=1)
    assert isinstance(results.best_config, dict)
    assert len(results.best_config["models"]) == 1
    assert results.df_trials["value"].values[0] > 0


def test_sdmetrics_metric(project: Project, data_source: Path):
    """Test the SDMetricsScore metric."""
    metric = SDMetricsScore(data_source=data_source)
    tuner = GretelHyperParameterTuner(ACTGANConfigSampler(), metric=metric)
    results = tuner.run(data_source=data_source, project=project, n_trials=1)
    assert isinstance(results.best_config, dict)
    assert len(results.best_config["models"]) == 1
    assert results.df_trials["value"].values[0] > 0
