import os
import platform

from pathlib import Path
from typing import Callable

import pandas as pd
import pytest

from gretel_client.config import RunnerMode
from gretel_client.evaluation.reports import _model_run_exc_message, ModelRunException
from gretel_client.evaluation.text_quality_report import TextQualityReport
from gretel_client.projects.projects import Project

from .conftest import pytest_skip_on_windows


@pytest.fixture
def data_source(get_fixture: Callable) -> Path:
    return get_fixture("amazon.csv")


@pytest.fixture
def ref_data(get_fixture: Callable) -> Path:
    return get_fixture("amazon.csv")


def test_manual_runner_mode_raises_an_exception(
    project: Project,
    data_source: Path,
    ref_data: Path,
    tmpdir: Path,
    runner_mode: RunnerMode = RunnerMode.MANUAL,
):
    with pytest.raises(ValueError) as err:
        TextQualityReport(
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
    report = TextQualityReport(
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
        "name": "evaluate-text-quality",
        "models": [
            {
                "evaluate": {
                    "task": {"type": "text"},
                    "data_source": "__tmp__",
                    "params": {
                        "target": None,
                        "sqs_report_rows": 80,
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
    report = TextQualityReport(
        project=project,
        name="my-preferred-name",
        data_source=data_source,
        ref_data=ref_data,
        output_dir=tmpdir,
        runner_mode=RunnerMode.CLOUD,
        target="text",
        record_count=1234,
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
                    "task": {"type": "text"},
                    "data_source": "__tmp__",
                    "params": {
                        "target": "text",
                        "sqs_report_rows": 1234,
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
    report = TextQualityReport(
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
    report = TextQualityReport(
        data_source=data_source,
        ref_data=ref_data,
    )
    assert report.project == None
    assert report.data_source
    assert report.ref_data
    assert report.output_dir == os.getcwd()
    assert report.runner_mode
    report.run()
    results = report.peek()
    assert set(results.keys()) == {"grade", "raw_score", "score"}
    assert results["grade"] == "Excellent"
    assert results["score"] > 90


def test_quality_report_with_dataframes(data_source):
    df = pd.read_csv(data_source)
    report = TextQualityReport(
        data_source=df,
        ref_data=df,
    )
    report.run()
    results = report.peek()
    assert set(results.keys()) == {"grade", "raw_score", "score"}
    assert results["grade"] == "Excellent"
    assert results["score"] > 90


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
    report = TextQualityReport(
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
