import os
import uuid

from pathlib import Path
from typing import Callable, Generator

import pytest

from gretel_client.gretel.exceptions import GretelJobSubmissionError
from gretel_client.gretel.interface import Gretel
from gretel_client.projects.jobs import Status


@pytest.fixture
def tabular_data_source(get_fixture: Callable) -> Path:
    return get_fixture("account-balances.csv")


@pytest.fixture(scope="module")
def gretel() -> Generator[Gretel, None, None]:
    gretel_obj = Gretel(
        project_name=f"pytest-transform-{uuid.uuid4().hex[:8]}",
        api_key=os.getenv("GRETEL_API_KEY"),
        endpoint="https://api-dev.gretel.cloud",
    )
    yield gretel_obj
    gretel_obj.get_project().delete()


def test_transform(
    gretel: Gretel,
    tabular_data_source: Path,
):
    config = """schema_version: "1.0"
name: "transform v2 test model"
models:
  - transform_v2:
      data_source: "_"
      globals:
        seed: 123
      classify:
        enabled: false
      steps:
        - rows:
            update:
              - name: CR
                value: fake(seed=this).bothify(text="###")
"""
    transform = gretel.submit_transform(
        config=config,
        data_source=tabular_data_source,
        job_label="testing123",
    )
    assert transform.job_status == Status.COMPLETED
    assert transform.transformed_df is not None
    assert "CR" in transform.transformed_df
    # Compare a subset of the transformation
    assert transform.transformed_df["CR"].to_list()[0:10] == [
        185,
        390,
        148,
        555,
        185,
        185,
        396,
        185,
        185,
        185,
    ]
    assert transform.report is not None

    # If the report type selection is broken this will fail
    str(transform.report)


def test_transform_errors(
    gretel: Gretel,
    tabular_data_source: Path,
):
    with pytest.raises(GretelJobSubmissionError):
        gretel.submit_transform(
            config="synthetics/navigator-ft", data_source=tabular_data_source
        )
