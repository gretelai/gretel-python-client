import json

from typing import Callable

from click.testing import CliRunner

from gretel_client.cli.cli import cli


def test_workflow_crud_from_cli(get_fixture: Callable, project: Callable):
    runner = CliRunner()

    # Create a workflow
    cmd = runner.invoke(
        cli,
        [
            "workflows",
            "create",
            "--name",
            "test-workflow",
            "--project",
            project.name,
            "--config",
            get_fixture("workflows/workflow.yaml"),
        ],
    )
    assert "Created workflow:" in cmd.output
    assert "test-workflow" in cmd.output
    assert cmd.exit_code == 0
    workflow_result = json.loads(cmd.output.rsplit("Created workflow:\n")[1])
    print(cmd.output)

    # Get workflows
    cmd = runner.invoke(
        cli,
        [
            "workflows",
            "list",
        ],
    )
    assert "Workflows:" in cmd.output
    assert workflow_result["id"] in cmd.output
    assert cmd.exit_code == 0

    # Get a workflow by id
    cmd = runner.invoke(
        cli,
        [
            "workflows",
            "get",
            "--id",
            workflow_result["id"],
        ],
    )
    assert "Workflow:" in cmd.output
    assert workflow_result["id"] in cmd.output
    assert cmd.exit_code == 0

    # Trigger a workflow by id
    cmd = runner.invoke(
        cli,
        [
            "workflows",
            "trigger",
            "--workflow-id",
            workflow_result["id"],
            "--wait",
            "10",
        ],
    )
    assert "Workflow run:" in cmd.output
    assert workflow_result["id"] in cmd.output
    assert "Workflow status is:" in cmd.output
    assert "Workflow run hasn't completed after waiting for 10 seconds." in cmd.output
    assert cmd.exit_code == 0
