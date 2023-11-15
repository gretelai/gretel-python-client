import os
import uuid

from pathlib import Path
from typing import Callable

import pytest

from gretel_client.gretel.config_setup import extract_model_config_section
from gretel_client.gretel.interface import Gretel


@pytest.fixture
def tuner_config(get_fixture: Callable) -> Path:
    return get_fixture("tuner/tuner_config_tabular.yml")


@pytest.fixture
def tabular_data_source(get_fixture: Callable) -> Path:
    return get_fixture("account-balances.csv")


@pytest.fixture(scope="module")
def gretel() -> Gretel:
    gretel = Gretel(
        project_name=f"pytest-tuner-{uuid.uuid4().hex[:8]}",
        api_key=os.getenv("GRETEL_API_KEY"),
        endpoint="https://api-dev.gretel.cloud",
    )
    yield gretel
    gretel.get_project().delete()


def test_tuner_tabular(gretel: Gretel, tabular_data_source: Path, tuner_config: Path):
    def callback(c):
        c["params"]["discriminator_dim"] = c["params"]["generator_dim"]
        return c

    tuned = gretel.run_tuner(
        tuner_config=tuner_config,
        data_source=tabular_data_source,
        n_jobs=1,
        n_trials=1,
        sampler_callback=callback,
    )

    assert isinstance(tuned.best_config, dict)
    assert len(tuned.trial_data) == 1

    _, c = extract_model_config_section(tuned.best_config)
    assert c["params"]["discriminator_dim"] == c["params"]["generator_dim"]
