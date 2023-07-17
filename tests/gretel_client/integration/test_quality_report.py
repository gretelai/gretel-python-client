import os
import platform

from pathlib import Path
from typing import Callable

import pandas as pd
import pytest

from gretel_client.config import RunnerMode
from gretel_client.evaluation.quality_report import QualityReport
from gretel_client.evaluation.reports import _model_run_exc_message, ModelRunException
from gretel_client.projects.projects import Project

from .conftest import pytest_skip_on_windows

INPUT_DF = pd.DataFrame([{"test_key": "test_value"}])


@pytest.fixture
def data_source(get_fixture: Callable) -> Path:
    return get_fixture("account-balances.csv")


@pytest.fixture
def ref_data(get_fixture: Callable) -> Path:
    return get_fixture("account-balances.csv")


def test_manual_runner_mode_raises_an_exception(
    project: Project,
    data_source: Path,
    ref_data: Path,
    tmpdir: Path,
    runner_mode: RunnerMode = RunnerMode.MANUAL,
):
    with pytest.raises(ValueError) as err:
        QualityReport(
            project=project,
            data_source=data_source,
            ref_data=ref_data,
            output_dir=tmpdir,
            runner_mode=runner_mode,
        )
    assert (
        str(err.value) == "Cannot use manual mode. Please use CLOUD, LOCAL, or HYBRID."
    )


@pytest.mark.parametrize(
    "runner_mode",
    [
        RunnerMode.CLOUD,
        RunnerMode.LOCAL,
    ],
)
def test_report_initialization_with_defaults(
    project: Project,
    data_source: Path,
    ref_data: Path,
    tmpdir: Path,
    runner_mode: RunnerMode,
):
    report = QualityReport(
        project=project,
        data_source=data_source,
        ref_data=ref_data,
        output_dir=tmpdir,
        runner_mode=runner_mode,
    )
    assert report.project
    assert report.data_source
    assert report.ref_data
    assert report.runner_mode
    assert report.output_dir
    assert report.model_config == {
        "schema_version": "1.0",
        "models": [
            {
                "evaluate": {
                    "data_source": "__tmp__",
                    "params": {
                        "correlation_columns": 75,
                        "mandatory_columns": [],
                        "sqs_report_columns": 250,
                        "sqs_report_rows": 5000,
                    },
                }
            }
        ],
    }


def test_report_initialization_with_custom_params(
    project: Project,
    data_source: Path,
    ref_data: Path,
    tmpdir: Path,
):
    report = QualityReport(
        project=project,
        name="my-preferred-name",
        data_source=data_source,
        ref_data=ref_data,
        output_dir=tmpdir,
        runner_mode=RunnerMode.CLOUD,
        mandatory_columns=["foo", "bar", "baz"],
    )
    assert report.project
    assert report.data_source
    assert report.ref_data
    assert report.runner_mode
    assert report.output_dir
    assert report.model_config == {
        "schema_version": "1.0",
        "name": "my-preferred-name",
        "models": [
            {
                "evaluate": {
                    "data_source": "__tmp__",
                    "params": {
                        "correlation_columns": 75,
                        "mandatory_columns": ["foo", "bar", "baz"],
                        "sqs_report_columns": 250,
                        "sqs_report_rows": 5000,
                    },
                }
            }
        ],
    }


def test_no_report_raises_exception(
    project: Project,
    data_source: Path,
    ref_data: Path,
    tmpdir: Path,
):
    report = QualityReport(
        project=project,
        data_source=data_source,
        ref_data=ref_data,
        output_dir=tmpdir,
    )
    with pytest.raises(ModelRunException) as err:
        report.as_dict
    assert str(err.value) == _model_run_exc_message
    with pytest.raises(ModelRunException) as err:
        report.as_html
    assert str(err.value) == _model_run_exc_message
    with pytest.raises(ModelRunException) as err:
        report.peek()
    assert str(err.value) == _model_run_exc_message


def test_no_project_provided_is_handled(
    data_source: Path,
    ref_data: Path,
):
    report = QualityReport(
        data_source=data_source,
        ref_data=ref_data,
    )
    assert report.project == None
    assert report.data_source
    assert report.ref_data
    assert report.output_dir == os.getcwd()
    assert report.runner_mode
    report.run()
    assert report.peek() == {"grade": "Excellent", "raw_score": 100.0, "score": 100}


def test_quality_report_with_dataframes(
    data_source: pd.DataFrame = INPUT_DF,
    ref_data: pd.DataFrame = INPUT_DF,
):
    report = QualityReport(
        data_source=data_source,
        ref_data=ref_data,
    )
    report.run()
    assert report.peek() == {"grade": "Excellent", "raw_score": 100.0, "score": 100}


@pytest_skip_on_windows
@pytest.mark.parametrize(
    "runner_mode",
    [
        (RunnerMode.CLOUD),
        (RunnerMode.LOCAL),
    ],
)
def test_hydrated_properties(
    project: Project,
    data_source: Path,
    ref_data: Path,
    tmpdir: Path,
    runner_mode: RunnerMode,
):
    report = QualityReport(
        project=project,
        data_source=data_source,
        ref_data=ref_data,
        output_dir=tmpdir,
        runner_mode=runner_mode,
    )
    report.run()
    assert report.peek()
    assert report.as_dict
    assert report.as_html
