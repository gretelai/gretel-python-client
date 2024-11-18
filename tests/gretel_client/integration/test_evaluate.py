import os
import uuid

from pathlib import Path
from typing import Callable, Generator

import pytest

from gretel_client.gretel.exceptions import GretelJobSubmissionError
from gretel_client.gretel.interface import Gretel
from gretel_client.projects.jobs import Status


@pytest.fixture
def data_source(get_fixture: Callable) -> Path:
    return get_fixture("account-balances.csv")


@pytest.fixture
def ref_data(get_fixture: Callable) -> Path:
    return get_fixture("account-balances-generated.csv")


@pytest.fixture(scope="module")
def gretel() -> Generator[Gretel, None, None]:
    gretel_obj = Gretel(
        project_name=f"pytest-evaluate-{uuid.uuid4().hex[:8]}",
        api_key=os.getenv("GRETEL_API_KEY"),
        endpoint="https://api-dev.gretel.cloud",
    )
    yield gretel_obj
    gretel_obj.get_project().delete()


def test_evaluate(
    gretel: Gretel,
    data_source: Path,
    ref_data: Path,
):
    config = """
    schema_version: "1.0"
    name: "evaluate test model"
    models:
    - evaluate:
        data_source: "_"
    """
    evaluate = gretel.submit_evaluate(
        config=config,
        data_source=data_source,
        ref_data=ref_data,
        job_label="testing123",
    )
    assert evaluate.job_status == Status.COMPLETED
    assert evaluate.evaluate_report is not None


def test_evaluate_errors(
    gretel: Gretel,
    data_source: Path,
    ref_data: Path,
):
    with pytest.raises(GretelJobSubmissionError):
        gretel.submit_evaluate(
            config="synthetics/navigator-ft",
            data_source=data_source,
            ref_data=ref_data,
        )
