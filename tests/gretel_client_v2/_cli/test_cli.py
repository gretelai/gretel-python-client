import os
from contextlib import contextmanager
from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from gretel_client_v2._cli.cli import cli as cli_entrypoint
from gretel_client_v2.config import (
    DEFAULT_GRETEL_ENDPOINT,
    DEFAULT_RUNNER,
    GRETEL_API_KEY,
    _ClientConfig,
    _load_config,
    configure_session,
)
from gretel_client_v2.projects.docker import _is_inside_container
from gretel_client_v2.projects.models import Model
from gretel_client_v2.projects.projects import Project, get_project


@pytest.fixture
def runner() -> CliRunner:
    """Returns a CliRunner that can be used to invoke the CLI from
    unit tests.
    """
    return CliRunner()


@pytest.fixture
def project(request) -> Project:
    """Returns a new project. This project will be cleaned up after
    the test runs.
    """
    p = get_project(create=True)
    request.addfinalizer(p.delete)
    return p


@pytest.fixture
def pre_trained_project() -> Project:
    yield get_project(name="gretel-client-project-pretrained")


@pytest.fixture
def trained_synth_model(pre_trained_project: Project) -> Model:
    return pre_trained_project.get_model(model_id="60b8f345e00f682f45819019")


@pytest.fixture
def trained_xf_model(pre_trained_project: Project) -> Model:
    return pre_trained_project.get_model(model_id="60b8f50ae00f682f4581901d")


@contextmanager
def clear_session_config():
    """Clears any session config so we can simulate a host without
    any existing Gretel config.
    """
    with patch.dict(os.environ, {}, clear=True):
        configure_session(_ClientConfig())
    yield
    configure_session(_load_config())


def test_cli(runner):
    cmd = runner.invoke(cli_entrypoint, ["--help"])
    assert cmd.exit_code == 0
    assert cmd.output.startswith("Usage")


@patch("gretel_client_v2._cli.cli.write_config")
def test_cli_does_configure(write_config: MagicMock, runner: CliRunner):
    cmd = runner.invoke(cli_entrypoint, ["configure"], input="\n\ngrtu...\n\n")
    assert not cmd.exception
    write_config.assert_called_once_with(
        _ClientConfig(
            endpoint="https://api.gretel.cloud",
            api_key="grtu...",
            default_project_name=None,
            default_runner=DEFAULT_RUNNER.value,
        )
    )


@patch("gretel_client_v2._cli.cli.write_config")
def test_cli_does_configure_with_project(
    write_config: MagicMock, runner: CliRunner, project: Project
):
    with clear_session_config():
        cmd = runner.invoke(
            cli_entrypoint,
            ["configure"],
            input=f"https://api-dev.gretel.cloud\n\n{os.getenv(GRETEL_API_KEY)}\n{project.name}\n",
            catch_exceptions=True,
        )
    assert not cmd.exception
    assert cmd.exit_code == 0
    write_config.assert_called_once_with(
        _ClientConfig(
            endpoint="https://api-dev.gretel.cloud",
            api_key=os.getenv(GRETEL_API_KEY),
            default_project_name=project.name,
        )
    )


@patch("gretel_client_v2._cli.cli.write_config")
def test_cli_does_fail_configure_with_bad_project(
    write_config: MagicMock, runner: CliRunner
):
    with clear_session_config():
        cmd = runner.invoke(
            cli_entrypoint,
            ["configure"],
            input=f"{DEFAULT_GRETEL_ENDPOINT}\n\n{os.getenv(GRETEL_API_KEY)}\nbad-project-key\n",
            catch_exceptions=True,
        )
        assert cmd.exit_code == 1


# mark: integration
def test_model_crud_from_cli(
    runner: CliRunner, project: Project, get_fixture: Callable, tmpdir: Path
):
    # 1. create a new model and run it locally.
    cmd = runner.invoke(
        cli_entrypoint,
        [
            "models",
            "create",
            "--config",
            str(get_fixture("transforms_config.yml")),
            "--project",
            project.project_id,
            "--runner",
            "local",
            "--output",
            str(tmpdir),
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0
    # 2. check that the model can be found via a search
    model = project.search_models()[0]
    model_id = model["uid"]
    assert model["status"] == "completed"
    assert not model["error_msg"]
    cmd = runner.invoke(
        cli_entrypoint,
        [
            "models",
            "search",
            "--project",
            project.project_id,
        ],
    )
    print(cmd.output)
    assert model_id in cmd.output
    assert cmd.exit_code == 0
    # 3. check that an existing model can be downloaded back to disk
    cmd = runner.invoke(
        cli_entrypoint,
        [
            "models",
            "get",
            "--model-id",
            model_id,
            "--project",
            project.project_id,
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0
    # 4. check that the model can be deleted
    cmd = runner.invoke(
        cli_entrypoint,
        [
            "models",
            "delete",
            "--model-id",
            model_id,
            "--project",
            project.project_id,
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0


@pytest.mark.skipif(_is_inside_container(), reason="running test from docker")
def test_model_crud_from_cli_local_inputs(
    runner: CliRunner, project: Project, get_fixture: Callable, tmpdir: Path
):
    # this test looks similar to test_model_crud_from_cli but will instead
    # test a training run using local inputs and outputs. currently, we don't
    # support mounting volumes when the cli is running inside a docker
    # container. the following check ensures that this test is only run
    # when the test harness is running on a non-docker host.

    # 1. create a new model and run it locally.
    cmd = runner.invoke(
        cli_entrypoint,
        [
            "models",
            "create",
            "--config",
            str(get_fixture("transforms_config.yml")),
            "--in-data",
            str(get_fixture("account-balances.csv")),
            "--output",
            str(tmpdir),
            "--project",
            project.project_id,
            "--runner",
            "local",
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0
    assert (tmpdir / "model.tar.gz").exists()
    assert (tmpdir / "model_logs.json.gz").exists()
    # 2. check that the model can be found via a search
    model = project.search_models()[0]
    model_id = model["uid"]

    assert model["status"] == "completed"
    assert not model["error_msg"]

    cmd = runner.invoke(
        cli_entrypoint,
        [
            "models",
            "search",
            "--project",
            project.project_id,
        ],
    )
    print(cmd.output)
    assert model_id in cmd.output
    assert cmd.exit_code == 0

    # 4. get records
    cmd = runner.invoke(
        cli_entrypoint,
        [
            "records",
            "transform",
            "--project",
            project.project_id,
            "--model-id",
            model_id,
            "--in-data",
            str(get_fixture("account-balances.csv")),
            "--output",
            str(tmpdir / "record_handler"),
            "--model-path",
            str(tmpdir / "model.tar.gz"),
            "--runner",
            "local"
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0
    assert (tmpdir / "record_handler/data.gz").exists()

    # 3. check that an existing model can be downloaded back to disk
    output_dir = tmpdir / "from_existing"
    cmd = runner.invoke(
        cli_entrypoint,
        [
            "models",
            "get",
            "--model-id",
            model_id,
            "--output",
            str(output_dir),
            "--project",
            project.project_id,
        ],
    )
    print(cmd.output)
    assert (output_dir / "logs.json.gz").exists()
    assert cmd.exit_code == 0

    # 4. check that the model can be deleted
    cmd = runner.invoke(
        cli_entrypoint,
        [
            "models",
            "delete",
            "--model-id",
            model_id,
            "--project",
            project.project_id,
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0


def test_local_model_params(runner: CliRunner, project: Project, get_fixture: Callable):
    base_cmd = [
        "models",
        "create",
        "--config",
        "synthetics/default",
        "--in-data",
        str(get_fixture("account-balances.csv")),
        "--runner",
        "local",
        "--dry-run",
        "--project",
        project.project_id,
    ]

    # assert that --runner=local and no output param results in an error
    cmd = runner.invoke(cli_entrypoint, base_cmd)
    assert cmd.exit_code == 2
    assert "Usage:" in cmd.output and "--output is not set" in cmd.output

    # check that --runner=local and --output params are ok
    cmd = runner.invoke(cli_entrypoint, base_cmd + ["--output", "tmp"])
    assert cmd.exit_code == 0

    # check that --wait cant be passed with an output dir
    cmd = runner.invoke(cli_entrypoint, base_cmd + ["--output", "tmp", "--wait", "10"])
    assert cmd.exit_code == 2 and "--wait is > 0" in cmd.output


@patch("gretel_client_v2._cli.common.get_project")
@patch("gretel_client_v2._cli.models.ContainerRun")
def test_local_model_upload_flag(
    container_run: MagicMock, get_project: MagicMock, runner: CliRunner
):
    """Checks to see artifacts are uploaded when --upload model is passed."""
    get_project.return_value.create_model.return_value.submit.return_value = {
        "model_key": ""
    }
    get_project.return_value.create_model.return_value._data = {}
    cmd = runner.invoke(
        cli_entrypoint,
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
    assert container_run.from_model.return_value.enable_cloud_uploads.call_count == 1


@patch("gretel_client_v2._cli.common.get_project")
@patch("gretel_client_v2._cli.models.ContainerRun")
def test_local_model_upload_disabled_by_default(
    container_run: MagicMock, get_project: MagicMock, runner: CliRunner
):
    get_project.return_value.create_model.return_value.submit.return_value = {
        "model_key": ""
    }
    get_project.return_value.create_model.return_value._data = {}
    runner.invoke(
        cli_entrypoint,
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
    assert container_run.return_value.enable_cloud_uploads.call_count == 0


def test_artifacts_crud(runner: CliRunner, project: Project, get_fixture: Callable):
    # upload an artifact
    cmd = runner.invoke(
        cli_entrypoint,
        [
            "artifacts",
            "upload",
            "--project",
            project.name,
            "--in-data",
            get_fixture("account-balances.csv"),
        ],
    )
    assert "gretel_" in cmd.output  # checks that a gretel key is returneds
    assert cmd.exit_code == 0
    assert len(project.artifacts) == 1
    # check that we can list the artifact
    cmd = runner.invoke(cli_entrypoint, ["artifacts", "list", "--project", project.name])
    assert "account-balances" in cmd.output
    assert cmd.exit_code == 0
    assert len(project.artifacts) == 1
    # check that we can delete the artifact
    cmd = runner.invoke(
        cli_entrypoint,
        [
            "artifacts",
            "delete",
            "--project",
            project.name,
            "--artifact-key",
            project.artifacts[0]["key"],
        ],
    )
    assert cmd.exit_code == 0
    assert len(project.artifacts) == 0


def test_artifact_invalid_data(
    runner: CliRunner, project: Project, get_fixture: Callable
):
    cmd = runner.invoke(
        cli_entrypoint,
        [
            "artifacts",
            "upload",
            "--project",
            project.name,
            "--in-data",
            get_fixture("invalid_data.json"),
        ],
    )
    assert (
        cmd.exit_code == 0
    )  # todo(dn): this should fail when we get better data validation checks


@pytest.mark.skipif(_is_inside_container(), reason="running test from docker")
def test_records_generate(
    runner: CliRunner, get_fixture: Callable, tmpdir: Path, trained_synth_model: Model
):
    cmd = runner.invoke(
        cli_entrypoint,
        [  # type:ignore
            "records",
            "generate",
            "--project",
            trained_synth_model.project.project_id,
            "--model-id",
            trained_synth_model.model_id,
            "--output",
            str(tmpdir),
            "--runner",
            "local",
        ],
    )
    assert cmd.exit_code == 0
    assert (tmpdir / "data.gz").exists()


@pytest.mark.skipif(_is_inside_container(), reason="running test from docker")
def test_records_transform(
    runner: CliRunner, get_fixture: Callable, tmpdir: Path, trained_xf_model: Model
):
    cmd = runner.invoke(
        cli_entrypoint,
        [  # type:ignore
            "records",
            "transform",
            "--project",
            trained_xf_model.project.project_id,
            "--model-id",
            trained_xf_model.model_id,
            "--in-data",
            str(get_fixture("account-balances.csv")),
            "--output",
            str(tmpdir),
            "--runner",
            "local",
        ],
    )
    assert cmd.exit_code == 0
    assert (tmpdir / "data.gz").exists()
