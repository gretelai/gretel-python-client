import pytest
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from gretel_client_v2._cli.cli import cli as cli_entrypoint
from gretel_client_v2._cli import cli
from gretel_client_v2._cli import projects
from gretel_client_v2.config import _ClientConfig


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_cli(runner):
    call = runner.invoke(cli_entrypoint, ["--help"])
    assert call.exit_code == 0
    assert call.output.startswith("Usage")


@patch.object(cli, "write_config")
def test_cli_does_configure(write_config: MagicMock, runner: CliRunner):
    call = runner.invoke(cli_entrypoint, ["configure"], input="\ngrtu...\n\n")
    assert not call.exception
    write_config.assert_called_once_with(
        _ClientConfig(
            endpoint="https://api-dev.gretel.cloud",
            api_key="grtu...",
            default_project_name=None,
        )
    )


@patch.object(projects, "validate_project")
@patch.object(projects, "write_config")
def test_project_set_default(
    write_config: MagicMock,
    validate_project: MagicMock,
    runner: CliRunner,
):
    validate_project.return_value = True

    call = runner.invoke(
        cli_entrypoint, ["projects", "set-default", "--name", "test-project"]
    )

    assert call.exit_code == 0
    validate_project.assert_called_once_with("test-project")
    client_config: _ClientConfig = write_config.call_args[0][0]
    assert client_config.default_project_name == "test-project"



@patch.object(projects, "validate_project")
@patch.object(projects, "write_config")
def test_project_set_default_not_exist(
    write_config: MagicMock, validate_project: MagicMock, runner: CliRunner
):
    validate_project.return_value = False

    call = runner.invoke(
        cli_entrypoint, ["projects", "set-default", "--name", "test-project"]
    )

    assert call.exit_code == 1
    write_config.assert_not_called()
    validate_project.assert_called_once_with("test-project")
