import os
import uuid

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from click.testing import CliRunner

from gretel_client.cli.cli import cli
from gretel_client.config import (
    _load_config,
    ClientConfig,
    configure_session,
    DEFAULT_GRETEL_ARTIFACT_ENDPOINT,
    DEFAULT_GRETEL_ENDPOINT,
    DEFAULT_RUNNER,
    GRETEL_API_KEY,
)
from gretel_client.projects.exceptions import GretelProjectError
from gretel_client.projects.projects import get_project, Project

from .conftest import print_cmd_output


@contextmanager
def clear_session_config():
    """Clears any session config so we can simulate a host without
    an existing Gretel config.
    """
    with patch.dict(os.environ, {}, clear=True):
        configure_session(ClientConfig())
    try:
        yield
    finally:
        configure_session(_load_config())


def test_cli(runner):
    cmd = runner.invoke(cli, ["--help"])
    assert cmd.exit_code == 0
    assert cmd.output.startswith("Usage")


@patch("gretel_client.cli.cli.write_config")
def test_cli_does_configure(write_config: MagicMock, runner: CliRunner):
    cmd = runner.invoke(cli, ["configure"], input="\n\n\ngrtu...\n\n")
    assert not cmd.exception
    write_config.assert_called_once_with(
        ClientConfig(
            endpoint="https://api-dev.gretel.cloud",
            api_key="grtu...",
            default_project_name=None,
            default_runner=DEFAULT_RUNNER.value,
            artifact_endpoint=None,
        )
    )


@patch("gretel_client.cli.cli.write_config")
def test_cli_does_configure_with_project(
    write_config: MagicMock, runner: CliRunner, project: Project
):
    with clear_session_config():
        cmd = runner.invoke(
            cli,
            ["configure"],
            input=f"https://api-dev.gretel.cloud\n\n\n{os.getenv(GRETEL_API_KEY)}\n{project.name}\n",
            catch_exceptions=True,
        )
    assert not cmd.exception
    assert cmd.exit_code == 0
    write_config.assert_called_once_with(
        ClientConfig(
            endpoint="https://api-dev.gretel.cloud",
            api_key=os.getenv(GRETEL_API_KEY),
            default_project_name=project.name,
            artifact_endpoint=None,
        )
    )


@patch("gretel_client.cli.cli.write_config")
def test_cli_does_configure_with_custom_artifact_endpoint_and_hybrid_runner(
    write_config: MagicMock, runner: CliRunner, project: Project
):
    with clear_session_config():
        cmd = runner.invoke(
            cli,
            ["configure"],
            input=f"https://api-dev.gretel.cloud\ns3://my-bucket\nhybrid\n{os.getenv(GRETEL_API_KEY)}\n\n",
            catch_exceptions=True,
        )
    assert not cmd.exception
    assert cmd.exit_code == 0
    write_config.assert_called_once_with(
        ClientConfig(
            endpoint="https://api-dev.gretel.cloud",
            api_key=os.getenv(GRETEL_API_KEY),
            default_project_name=None,
            default_runner="hybrid",
            artifact_endpoint="s3://my-bucket",
        )
    )


@patch("gretel_client.cli.cli.write_config")
def test_cli_fails_configure_with_custom_artifact_endpoint_and_default_cloud_runner(
    write_config: MagicMock, runner: CliRunner, project: Project
):
    with clear_session_config():
        cmd = runner.invoke(
            cli,
            ["configure"],
            input=f"https://api-dev.gretel.cloud\ns3://my-bucket\n\n{os.getenv(GRETEL_API_KEY)}\n\n",
            catch_exceptions=True,
        )
    assert cmd.exit_code == 1
    assert "custom artifact endpoint with cloud runner" in cmd.stderr


@patch("gretel_client.cli.cli.write_config")
def test_cli_does_pass_configure_with_bad_project(
    write_config: MagicMock, runner: CliRunner
):
    with clear_session_config():
        cmd = runner.invoke(
            cli,
            ["configure"],
            input=f"{DEFAULT_GRETEL_ENDPOINT}\n\n{os.getenv(GRETEL_API_KEY)}\nbad-project-key\n",
            catch_exceptions=True,
        )
        assert cmd.exit_code == 0


def test_missing_api_key(runner: CliRunner):
    with clear_session_config():
        cmd = runner.invoke(cli, ["projects", "create", "--name", "foo"])

        assert cmd.exit_code == 1
        assert "Gretel API key was not set" in cmd.stderr


def test_invalid_api_key(runner: CliRunner):
    with clear_session_config():
        configure_session(ClientConfig(api_key="invalid"))

        cmd = runner.invoke(cli, ["projects", "create", "--name", "foo"])
        assert cmd.exit_code == 1
        assert "Invalid Gretel API key" in cmd.stderr


def test_invalid_project_create(runner: CliRunner):
    project_name = f"{uuid.uuid4().hex[:5]}_not_dns_compliant"
    cmd = runner.invoke(cli, ["--debug", "projects", "create", "--name", project_name])
    assert cmd.exit_code == 1


def test_can_create_project(runner: CliRunner, request):
    project_name = f"test-{uuid.uuid4().hex[:5]}"
    request.addfinalizer(lambda: get_project(name=project_name).delete())
    cmd = runner.invoke(
        cli,
        [
            "--debug",
            "projects",
            "create",
            "--name",
            project_name,
            "--display-name",
            "test-project",
            "--desc",
            "test description",
        ],
    )
    print_cmd_output(cmd)
    assert cmd.exit_code == 0


def test_cannot_delete_without_name_or_uid(runner: CliRunner):
    cmd = runner.invoke(
        cli,
        [
            "--debug",
            "projects",
            "delete",
        ],
    )
    assert cmd.exit_code == 2
    assert "Please use --name or --uid option." in cmd.stderr


def test_cannot_delete_project_with_both_name_and_uid(runner: CliRunner, request):
    project = get_project(create=True)
    request.addfinalizer(lambda: get_project(name=project.name).delete())
    cmd = runner.invoke(
        cli,
        [
            "--debug",
            "projects",
            "delete",
            "--name",
            project.name,
            "--uid",
            project.project_id,
        ],
    )
    assert cmd.exit_code == 2
    assert (
        "Cannot pass both --uid and --name. Please use --name or --uid option."
        in cmd.stderr
    )


def test_cannot_delete__non_existing_project(runner: CliRunner):
    cmd = runner.invoke(
        cli,
        [
            "--debug",
            "projects",
            "delete",
            "--name",
            "non_existing_project",
        ],
    )
    assert cmd.exit_code == 1
    assert "Could not get project using 'non_existing_project'." in cmd.stderr


def test_can_delete_project(runner: CliRunner):
    project = get_project(create=True)
    cmd = runner.invoke(
        cli,
        [
            "--debug",
            "projects",
            "delete",
            "--name",
            project.name,
        ],
    )
    assert cmd.exit_code == 0
    assert "Project was deleted." in cmd.stderr
    with pytest.raises(GretelProjectError):
        get_project(name=project.name)
