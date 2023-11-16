import os
import uuid

from pathlib import Path
from typing import Callable

import pandas as pd
import pytest

from gretel_client.gretel.exceptions import (
    GretelJobSubmissionError,
    GretelProjectNotSetError,
)
from gretel_client.gretel.interface import Gretel

NUM_RECORDS = 100


@pytest.fixture
def data_source(get_fixture: Callable) -> Path:
    return get_fixture("gretel/us-adult-income.csv")


@pytest.fixture
def seed_data_file_path(get_fixture: Callable) -> Path:
    return get_fixture("gretel/us-adult-income_seed-data.csv")


@pytest.fixture(scope="module")
def gretel() -> Gretel:
    gretel = Gretel(
        project_name=f"pytest-tabular-{uuid.uuid4().hex[:8]}",
        api_key=os.getenv("GRETEL_API_KEY"),
        endpoint="https://api-dev.gretel.cloud",
    )
    yield gretel
    gretel._project.delete()


@pytest.mark.parametrize(
    "config_template,custom_settings",
    [
        (
            "tabular-actgan",
            {
                "params": {
                    "epochs": 20,
                    "generator_dim": [128, 128],
                    "discriminator_dim": [128, 128],
                },
                "privacy_filters": {"similarity": "high", "outliers": "medium"},
            },
        ),
    ],
)
def test_gretel_submit_train_tabular(
    gretel: Gretel,
    data_source: Path,
    config_template: str,
    custom_settings: dict,
):
    """Test training with custom tabular configs."""

    # Test training job.
    trained = gretel.submit_train(
        config_template, data_source=data_source, **custom_settings
    )
    assert trained.job_status == "completed"
    assert trained.report.quality_scores["synthetic_data_quality_score"] > 0

    # Verify that the model config was updated with the custom settings.
    for section, settings in custom_settings.items():
        model_type = list(trained.model_config["models"][0].keys())[0]
        for k, v in settings.items():
            assert trained.model_config["models"][0][model_type][section][k] == v


def test_gretel_submit_generate_tabular(gretel: Gretel):
    """Test tabular synthetic data generation."""

    # Fetch model from previous test.
    model = list(gretel.get_project().search_models(model_name="tabular-actgan"))[0]
    generated = gretel.submit_generate(model.model_id, num_records=NUM_RECORDS)
    assert generated.job_status == "completed"


def test_gretel_submit_generate_from_seed_file_tabular(
    gretel: Gretel, seed_data_file_path: Path
):
    """Test tabular synthetic data generation from seed data."""
    model = list(gretel.get_project().search_models(model_name="tabular-actgan"))[0]
    generated = gretel.submit_generate(model.model_id, seed_data=seed_data_file_path)
    assert generated.job_status == "completed"
    assert len(generated.synthetic_data) == len(pd.read_csv(seed_data_file_path))


def test_gretel_submit_generate_from_seed_dataframe_tabular(
    gretel: Gretel, seed_data_file_path: Path
):
    """Test tabular synthetic data generation from seed data."""
    model = list(gretel.get_project().search_models(model_name="tabular-actgan"))[0]
    df_seed = pd.read_csv(seed_data_file_path)
    generated = gretel.submit_generate(model.model_id, seed_data=df_seed)
    assert generated.job_status == "completed"
    assert len(generated.synthetic_data) == len(df_seed)
    assert generated.synthetic_data["education"].nunique() == 1
    assert generated.synthetic_data["education"].unique()[0] == "Doctorate"


def test_gretel_fetch_train_job_results(gretel: Gretel):
    """Test fetching train job results."""
    model = list(gretel.get_project().search_models(model_name="tabular-actgan"))[0]
    results = gretel.fetch_train_job_results(model.model_id)
    assert results.model_id == model.model_id
    assert results.job_status == "completed"
    assert results.report.quality_scores["synthetic_data_quality_score"] > 0


def test_gretel_fetch_generate_job_results(gretel: Gretel):
    """Test fetching generate job results."""
    model = list(gretel.get_project().search_models(model_name="tabular-actgan"))[0]
    record_handler = list(model.get_record_handlers())[0]
    results = gretel.fetch_generate_job_results(
        model.model_id, record_handler.record_id
    )
    assert results.model_id == model.model_id
    assert results.record_id == record_handler.record_id
    assert results.job_status == "completed"
    assert len(results.synthetic_data) == NUM_RECORDS
    assert isinstance(results.synthetic_data_link, str)


def test_gretel_submit_generate_invalid_arguments(gretel: Gretel):
    """Test that submit_generate raises exception when given invalid arguments."""
    model = list(gretel.get_project().search_models(model_name="tabular-actgan"))[0]
    with pytest.raises(GretelJobSubmissionError):
        gretel.submit_generate(model.model_id)
    with pytest.raises(GretelJobSubmissionError):
        gretel.submit_generate(
            model.model_id, num_records=10, seed_data=seed_data_file_path
        )


@pytest.mark.parametrize(
    "base_config",
    [
        "amplify",
        "tabular-actgan",
        "tabular-differential-privacy",
        "tabular-lstm",
        "time-series",
    ],
)
def test_gretel_train_no_data_source_exception(gretel: Gretel, base_config: str):
    """Test that an error is raised when a data source is required but not provided."""
    with pytest.raises(GretelJobSubmissionError):
        gretel.submit_train(base_config, data_source=None)


def test_gretel_no_project_set_exceptions():
    gretel = Gretel(
        api_key=os.getenv("GRETEL_API_KEY"),
        endpoint="https://api-dev.gretel.cloud",
    )

    assert gretel._project is None

    with pytest.raises(GretelProjectNotSetError):
        gretel.fetch_model(model_id="1234")

    with pytest.raises(GretelProjectNotSetError):
        gretel.fetch_train_job_results(model_id="1234")

    with pytest.raises(GretelProjectNotSetError):
        gretel.fetch_generate_job_results(model_id="1234", record_id="1234")
