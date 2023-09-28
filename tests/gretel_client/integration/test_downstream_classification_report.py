from pathlib import Path
from typing import Callable

import pandas as pd
import pytest

from gretel_client.config import RunnerMode
from gretel_client.evaluation.downstream_classification_report import (
    DownstreamClassificationReport,
)
from gretel_client.evaluation.reports import _model_run_exc_message, ModelRunException
from gretel_client.projects.projects import Project

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
        DownstreamClassificationReport(
            target="foo",
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
    report = DownstreamClassificationReport(
        target="foo",
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
        "name": "evaluate-downstream-classification-model",
        "models": [
            {
                "evaluate": {
                    "task": {"type": "classification"},
                    "data_source": "__tmp__",
                    "params": {
                        "target": "foo",
                        "holdout": 0.2,
                        "models": [],
                        "metric": "acc",
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
    report = DownstreamClassificationReport(
        target="foo",
        holdout=0.3,
        models=["lr"],
        metric="auc",
        project=project,
        name="my-preferred-name",
        data_source=data_source,
        ref_data=ref_data,
        test_data="test/data/path",
        output_dir=tmpdir,
        runner_mode=RunnerMode.CLOUD,
        record_count=6543,
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
                    "task": {"type": "classification"},
                    "data_source": "__tmp__",
                    "params": {
                        "target": "foo",
                        "holdout": 0.3,
                        "models": ["lr"],
                        "metric": "auc",
                        "sqs_report_rows": 6543,
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
    report = DownstreamClassificationReport(
        target="foo",
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


def test_sane_target_and_holdout(
    project: Project,
    data_source: Path,
    ref_data: Path,
    tmpdir: Path,
):
    with pytest.raises(ValueError) as err:
        report = DownstreamClassificationReport(
            target=None,
            project=project,
            data_source=data_source,
            ref_data=ref_data,
            output_dir=tmpdir,
        )
    assert str(err.value) == "A nonempty target is required."
    with pytest.raises(ValueError) as err:
        report = DownstreamClassificationReport(
            target="foo",
            holdout=1.1,
            project=project,
            data_source=data_source,
            ref_data=ref_data,
            output_dir=tmpdir,
        )
    assert str(err.value) == "Holdout must be between 0.0 and 1.0."
