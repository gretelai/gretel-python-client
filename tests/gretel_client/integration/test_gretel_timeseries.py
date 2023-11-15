import os
import uuid

from pathlib import Path
from typing import Callable

import pytest

from gretel_client.gretel.interface import Gretel

NUM_RECORDS = 1000


@pytest.fixture
def data_file_path(get_fixture: Callable) -> Path:
    return get_fixture("gretel/daily-website-visitors.csv")


@pytest.fixture(scope="module")
def gretel() -> Gretel:
    gretel = Gretel(
        project_name=f"pytest-timeseries-{uuid.uuid4().hex[:8]}",
        api_key=os.getenv("GRETEL_API_KEY"),
        endpoint="https://api-dev.gretel.cloud",
    )
    yield gretel
    gretel._project.delete()


def test_train_timeseries_custom_config(gretel: Gretel, data_file_path: Path):
    """Test training a timeseries model with a custom config."""
    trained = gretel.submit_train(
        base_config="time-series",
        data_source=data_file_path,
        params={"epochs": 50},
        generate={"num_records": NUM_RECORDS},
    )
    config = trained.model_config["models"][0]["timeseries_dgan"]["params"]
    assert trained.job_status == "completed"
    assert config["epochs"] == 50
    assert trained.report is None
    assert (
        len(trained.fetch_report_synthetic_data())
        == config["max_sequence_len"] * NUM_RECORDS
    )


def test_generate_timeseries_data(gretel: Gretel):
    """Test generating timeseries data from a trained model."""
    model = list(gretel.get_project().search_models(model_name="time-series-dgan"))[0]
    generated = gretel.submit_generate(model.model_id, num_records=NUM_RECORDS)
    config = model.model_config["models"][0]["timeseries_dgan"]["params"]
    assert generated.job_status == "completed"
    assert len(generated.synthetic_data) == config["max_sequence_len"] * NUM_RECORDS
