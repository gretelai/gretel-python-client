from pathlib import Path
from unittest.mock import MagicMock, patch
from typing import Callable

import pytest
from click.testing import CliRunner

from gretel_client.cli.cli import cli
from gretel_client.cli.common import download_artifacts
from gretel_client.projects.jobs import Status


@pytest.fixture
def runner() -> CliRunner:
    """Returns a CliRunner that can be used to invoke the CLI from
    unit tests.
    """
    return CliRunner()


@pytest.fixture
def get_project() -> MagicMock:
    with patch("gretel_client.cli.common.get_project") as get_project:
        get_project.return_value.create_model.return_value.create.return_value = {
            "model_key": ""
        }
        get_project.return_value.create_model.return_value.print_obj = {}
        get_project.return_value.create_model.return_value.billing_details = {}
        get_project.return_value.create_model.return_value.peek_report.return_value = {}
        get_project.return_value.create_model.return_value.status = Status.COMPLETED
        get_project.return_value.create_model.return_value._data = {}
        yield get_project


@pytest.fixture
def container_run() -> MagicMock:
    with patch("gretel_client.cli.models.ContainerRun") as container_run:
        yield container_run


def test_local_model_upload_flag(
    container_run: MagicMock, get_project: MagicMock, runner: CliRunner
):
    cmd = runner.invoke(
        cli,
        [
            "models",
            "create",
            "--upload-model",
            "--runner",
            "local",
            "--config",
            "synthetics/default",
            "--output",
            "tmp",
            "--project",
            "mocked",
        ],
    )
    assert cmd.exit_code == 0
    assert container_run.from_job.return_value.start.call_count == 1
    assert container_run.from_job.return_value.enable_cloud_uploads.call_count == 1


def test_local_model_upload_disabled_by_default(
    container_run: MagicMock, get_project: MagicMock, runner: CliRunner
):
    get_project.return_value.create_model.return_value.create.return_value = {
        "model_key": ""
    }
    get_project.return_value.create_model.return_value.print_obj = {}
    get_project.return_value.create_model.return_value.billing_details = {}
    get_project.return_value.create_model.return_value.peek_report.return_value = {}
    get_project.return_value.create_model.return_value.status = Status.COMPLETED
    get_project.return_value.create_model.return_value._data = {}
    cmd = runner.invoke(
        cli,
        [
            "models",
            "create",
            "--runner",
            "local",
            "--config",
            "synthetics/default",
            "--output",
            "tmp",
            "--project",
            "mocked",
        ],
    )
    assert cmd.exit_code == 0
    assert container_run.from_job.return_value.start.call_count == 1
    assert container_run.from_job.return_value.enable_cloud_uploads.call_count == 0


def test_does_write_artifacts_to_disk(tmpdir: Path, get_fixture: Callable):
    sc = MagicMock()
    job = MagicMock()
    base_endpoint = (
        "https://gretel-public-website.s3.us-west-2.amazonaws.com/tests/client/"
    )
    files = ["account-balances.csv", "report_json.json.gz", "model.tar.gz"]
    keys = ["data", "report_json", "model"]
    job.get_artifacts.return_value = iter(zip(keys, [base_endpoint + f for f in files]))
    download_artifacts(sc, str(tmpdir), job)
    for file in files:
        assert (tmpdir / file).exists()
