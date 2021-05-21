import os
from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock, patch
from contextlib import contextmanager

import pytest
from click.testing import CliRunner

from gretel_client_v2._cli.cli import cli as cli_entrypoint
from gretel_client_v2.config import (
    DEFAULT_GRETEL_ENDPOINT,
    _ClientConfig,
    _load_config,
    configure_session,
    GRETEL_API_KEY,
)
from gretel_client_v2.projects.projects import Project, get_project
from gretel_client_v2.projects.docker import _is_inside_container


@pytest.fixture
def runner() -> CliRunner:
    """Returns a CliRuner that can be used to invoke the CLI from
    unit tests.
    """
    return CliRunner()


@pytest.fixture
def project() -> Project:
    """Returns a new project. This project will be cleaned up after
    the test runs.
    """
    p = get_project(create=True)
    yield p
    p.delete()


@pytest.fixture
def session_config():
    """Ensures the the host client config is reset after each test."""
    yield
    configure_session(_load_config())


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


@patch("gretel_client_v2._cli.cli.write_config")
def test_cli_does_configure_with_project(
    write_config: MagicMock, runner: CliRunner, project: Project
):
    with clear_session_config():
        call = runner.invoke(
            cli_entrypoint,
            ["configure"],
            input=f"{DEFAULT_GRETEL_ENDPOINT}\n{os.getenv(GRETEL_API_KEY)}\n{project.name}\n",
            catch_exceptions=True
        )
    assert not call.exception
    assert call.exit_code == 0
    write_config.assert_called_once_with(
        _ClientConfig(
            endpoint=DEFAULT_GRETEL_ENDPOINT,
            api_key=os.getenv(GRETEL_API_KEY),
            default_project_name=project.name,
        )
    )


@patch("gretel_client_v2._cli.cli.write_config")
def test_cli_does_fail_configure_with_bad_project(
    write_config: MagicMock, runner: CliRunner
):
    with clear_session_config():
        call = runner.invoke(
            cli_entrypoint,
            ["configure"],
            input=f"{DEFAULT_GRETEL_ENDPOINT}\n{os.getenv(GRETEL_API_KEY)}\nbad-project-key\n",
            catch_exceptions=True
        )
        assert call.exit_code == 1


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
