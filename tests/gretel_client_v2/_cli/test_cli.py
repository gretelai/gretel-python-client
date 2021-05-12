from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from gretel_client_v2._cli.cli import cli as cli_entrypoint
from gretel_client_v2.config import _ClientConfig, _load_config, configure_session
from gretel_client_v2.projects.projects import Project, get_project
from gretel_client_v2.projects.docker import _is_inside_container


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def project():
    p = get_project(create=True)
    yield p
    p.delete()


@pytest.fixture
def session_config():
    yield
    configure_session(_load_config())


def test_cli(runner):
    call = runner.invoke(cli_entrypoint, ["--help"])
    assert call.exit_code == 0
    assert call.output.startswith("Usage")


@patch("gretel_client_v2._cli.cli.write_config")
def test_cli_does_configure(write_config: MagicMock, runner: CliRunner, session_config):
    call = runner.invoke(cli_entrypoint, ["configure"], input="\ngrtu...\n\n")
    assert not call.exception
    write_config.assert_called_once_with(
        _ClientConfig(
            endpoint="https://api-dev.gretel.cloud",
            api_key="grtu...",
            default_project_name=None,
        )
    )


@patch("gretel_client_v2._cli.projects.validate_project")
@patch("gretel_client_v2._cli.projects.write_config")
def test_project_set_default(
    write_config: MagicMock,
    validate_project: MagicMock,
    runner: CliRunner,
    session_config,
):
    validate_project.return_value = True
    call = runner.invoke(
        cli_entrypoint, ["projects", "set-default", "--name", "test-project"]
    )
    assert call.exit_code == 0
    validate_project.assert_called_once_with("test-project")
    client_config: _ClientConfig = write_config.call_args[0][0]
    assert client_config.default_project_name == "test-project"


@patch("gretel_client_v2._cli.projects.validate_project")
@patch("gretel_client_v2._cli.projects.write_config")
@patch("gretel_client_v2._cli.common.get_project")
def test_project_set_default_not_exist(
    _: MagicMock,
    write_config: MagicMock,
    validate_project: MagicMock,
    runner: CliRunner,
):
    validate_project.return_value = False
    call = runner.invoke(
        cli_entrypoint,
        ["projects", "set-default", "--name", "test-project"],
        catch_exceptions=False,
    )
    assert call.exit_code == 1
    write_config.assert_not_called()
    validate_project.assert_called_once_with("test-project")


# mark: integration
def test_model_crud_from_cli(
    runner: CliRunner, project: Project, get_fixture: Callable
):
    # 1. create a new model and run it locally.
    call = runner.invoke(
        cli_entrypoint,
        [
            "models",
            "create",
            "--config",
            str(get_fixture("transforms_config.yml")),
            "--project",
            project.project_id,
        ],
    )
    print(call.output)
    assert call.exit_code == 0
    # 2. check that the model can be found via a search
    model = project.search_models()[0]
    model_id = model["uid"]
    assert model["status"] == "completed"
    assert not model["error_msg"]
    call = runner.invoke(
        cli_entrypoint,
        [
            "models",
            "search",
            "--project",
            project.project_id,
        ],
    )
    print(call.output)
    assert model_id in call.output
    assert call.exit_code == 0
    # 3. check that an existing model can be downloaded back to disk
    call = runner.invoke(
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
    print(call.output)
    assert call.exit_code == 0
    # 4. check that the model can be deleted
    call = runner.invoke(
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
    print(call.output)
    assert call.exit_code == 0


def test_model_crud_from_cli_local_inputs(
    runner: CliRunner, project: Project, get_fixture: Callable, tmpdir: Path
):
    # this test looks similar to test_model_crud_from_cli but will  instead
    # test a training run using local inputs and outputs. currently, we don't
    # support mounting volumes when the cli is running inside a docker
    # container. the following check ensures that this test is only run
    # when the test harness is running on a non-docker host.
    if _is_inside_container():
        return
    # 1. create a new model and run it locally.
    call = runner.invoke(
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
        ],
    )
    print(call.output)
    assert call.exit_code == 0
    assert (tmpdir / "model.tar.gz").exists()
    assert (tmpdir / "model_logs.json.gz").exists()
    # 2. check that the model can be found via a search
    model = project.search_models()[0]
    model_id = model["uid"]

    assert model["status"] == "completed"
    assert not model["error_msg"]

    call = runner.invoke(
        cli_entrypoint,
        [
            "models",
            "search",
            "--project",
            project.project_id,
        ],
    )
    print(call.output)
    assert model_id in call.output
    assert call.exit_code == 0
    # 3. check that an existing model can be downloaded back to disk
    output_dir = tmpdir / "from_existing"
    call = runner.invoke(
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
    print(call.output)
    assert (output_dir / "model.tar.gz").exists()
    assert call.exit_code == 0
    # 4. check that the model can be deleted
    call = runner.invoke(
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
    print(call.output)
    assert call.exit_code == 0
