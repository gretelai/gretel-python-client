import os

from typing import Callable
from unittest.mock import ANY, MagicMock, patch

import pytest

from click.testing import CliRunner

from gretel_client.cli.cli import cli
from gretel_client.projects import Project
from gretel_client.rest_v1.models import CreateConnectionRequest

PROJ_GUID = "proj_12312312"


@pytest.fixture
def get_project() -> MagicMock:
    with patch("gretel_client.cli.common.get_project") as get_project:
        get_project.return_value = Project(
            name="my-project", project_id="abc", project_guid=PROJ_GUID
        )

        yield get_project


@pytest.fixture
def runner() -> CliRunner:
    """Returns a CliRunner that can be used to invoke the CLI from
    unit tests.
    """
    return CliRunner()


@pytest.mark.parametrize("project", [PROJ_GUID, "my-project", "abc"])
@patch("gretel_client.cli.connections.get_connections_api")
def test_create_connection_project_guid(
    get_connections_api: MagicMock,
    get_project: MagicMock,
    get_fixture: Callable,
    project: str,
    runner: CliRunner,
):
    connection_file = get_fixture("connections/test_connection.json")
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project,
            "--from-file",
            connection_file,
        ],
    )
    assert "Created connection" in cmd.output
    assert cmd.exit_code == 0
    get_project.assert_called_once_with(name=project)
    get_connections_api.return_value.create_connection.assert_called_with(
        CreateConnectionRequest(
            project_id=PROJ_GUID,
            name="integration_test",
            type="test",
            credentials={"name": "test", "secret": "secret"},
            encrypted_credentials=None,
            config=None,
            connection_target_type=None,
        )
    )
