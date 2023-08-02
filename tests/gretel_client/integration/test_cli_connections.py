import json

from typing import Callable

from click.testing import CliRunner

from gretel_client.cli.cli import cli
from gretel_client.projects.projects import Project


def test_connection_crud_from_cli(get_fixture: Callable, project: Project):
    runner = CliRunner()

    # Create a connection
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection.json"),
        ],
    )
    assert "Created connection:" in cmd.output
    assert project.project_guid in cmd.output  # type: ignore
    assert cmd.exit_code == 0
    connection_result = json.loads(cmd.output.rsplit("Created connection:\n")[1])
    print(cmd.output)

    # Get connections
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "list",
        ],
    )
    assert "Connections:" in cmd.output
    assert connection_result["id"] in cmd.output
    assert cmd.exit_code == 0

    # Get a connection by id
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "get",
            "--id",
            connection_result["id"],
        ],
    )
    assert "Connection:" in cmd.output
    assert connection_result["id"] in cmd.output
    assert cmd.exit_code == 0

    # Update a connection by id
    # todo: reenable once PLAT-588 lands
    # cmd = runner.invoke(
    #     cli,
    #     [
    #         "connections",
    #         "update",
    #         "--id",
    #         connection_result["id"],
    #         "--from-file",
    #         get_fixture("connections/aws_connection.json"),
    #     ],
    # )
    # assert "Updated connection:" in cmd.output
    # assert "unit_test_name_edited" in cmd.output
    # assert "AWS" in cmd.output
    # assert cmd.exit_code == 0

    # Delete a connection by id
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "delete",
            "--id",
            connection_result["id"],
        ],
    )
    assert "Deleted connection " + connection_result["id"] in cmd.output
    assert cmd.exit_code == 0
