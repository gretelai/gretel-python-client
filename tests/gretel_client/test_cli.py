import os
from pathlib import Path
from unittest.mock import MagicMock, patch
from typing import Callable

import pytest
from click.testing import CliRunner

from gretel_client.cli.cli import cli
from gretel_client.cli.common import ModelObjectReader, download_artifacts
from gretel_client.config import (
    ClientConfig,
    GRETEL_API_KEY,
    GRETEL_CONFIG_FILE,
    GRETEL_ENDPOINT,
    GRETEL_PROJECT,
    configure_session,
    get_session_config,
    _load_config,
)
from gretel_client.projects.jobs import Status


@pytest.fixture
def runner() -> CliRunner:
    """Returns a CliRunner that can be used to invoke the CLI from
    unit tests.
    """
    return CliRunner()


@patch("gretel_client.cli.cli.write_config")
def test_configure_env(write_config: MagicMock, runner: CliRunner):
    orig_api, orig_proj, orig_endpoint = "orig_api", "orig_proj", "orig_endpoint"
    new_api, new_proj, new_endpoint = "new_api", "new_proj", "new_endpoint"

    with patch.dict(
        os.environ,
        {
            GRETEL_API_KEY: orig_api,
            GRETEL_ENDPOINT: orig_endpoint,
            GRETEL_PROJECT: orig_proj,
            GRETEL_CONFIG_FILE: "none"
        },
    ):
        configure_session(ClientConfig())
        assert get_session_config().api_key == orig_api
        assert get_session_config().endpoint == orig_endpoint
        assert get_session_config().default_project_name == orig_proj
        cmd = runner.invoke(
            cli,
            [
                "configure",
                "--api-key",
                new_api,
                "--endpoint",
                new_endpoint,
                "--project",
                new_proj,
            ],
        )

    assert get_session_config().api_key == new_api
    assert get_session_config().endpoint == new_endpoint
    assert get_session_config().default_project_name == new_proj
    assert cmd.exit_code == 0


@pytest.fixture
def get_project() -> MagicMock:
    with patch("gretel_client.cli.common.get_project") as get_project:
        get_project.return_value.create_model_obj.return_value.submit.return_value = {
            "model_key": ""
        }
        get_project.return_value.create_model_obj.return_value.print_obj = {}
        get_project.return_value.create_model_obj.return_value.billing_details = {}
        get_project.return_value.create_model_obj.return_value.peek_report.return_value = (
            {}
        )
        get_project.return_value.create_model_obj.return_value.status = Status.COMPLETED
        get_project.return_value.create_model_obj.return_value._data = {}
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
    get_project.return_value.create_model_obj.return_value.submit.return_value = {
        "model_key": ""
    }
    get_project.return_value.create_model_obj.return_value.print_obj = {}
    get_project.return_value.create_model_obj.return_value.billing_details = {}
    get_project.return_value.create_model_obj.return_value.peek_report.return_value = {}
    get_project.return_value.create_model_obj.return_value.status = Status.COMPLETED
    get_project.return_value.create_model_obj.return_value._data = {}
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


def test_does_read_model_json(runner: CliRunner, get_fixture: Callable):
    output = get_fixture("xf_model_create_output.json")
    sc = MagicMock()
    sc.model.data_source = "test.csv"
    model_obj = ModelObjectReader(output)
    model_obj.apply(sc)
    sc.in_data == "test.csv"
    sc.runner == "local"
    sc.set_project.assert_called_once_with("60b9a37000f67523d00b944c")
    sc.set_model.assert_called_once_with("60dca3d09c03f7c6edadee91")


def test_does_read_model_object_id():
    sc = MagicMock()
    model_obj = ModelObjectReader("test_id")
    model_obj.apply(sc)
    sc.set_model.assert_called_once_with("test_id")
    sc.data_source = None
    sc.runner = None


def test_does_read_model_object_str(get_fixture: Callable):
    output = get_fixture("xf_model_create_output.json")
    model_obj = ModelObjectReader(output.read_text())
    assert model_obj.model.get("uid") == "60dca3d09c03f7c6edadee91"
