import json
import os

from signal import SIGINT
from threading import Timer
from typing import Callable

from click.testing import CliRunner

from gretel_client.cli.cli import cli

from .conftest import pytest_skip_on_windows


@pytest_skip_on_windows
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
            "--runner_mode",
            "cloud",
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

    # Update a workflow
    cmd = runner.invoke(
        cli,
        [
            "workflows",
            "update",
            "--workflow-id",
            workflow_result["id"],
            "--config",
            get_fixture("workflows/workflow_updated.yaml"),
        ],
    )
    assert "Updated workflow:" in cmd.output
    assert "charles" in cmd.output
    assert cmd.exit_code == 0

    # Run a workflow by id
    cmd = runner.invoke(
        cli,
        [
            "workflows",
            "run",
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

    # run a workflow by id, then cancel it
    # Send an interrupt signal after 3 seconds
    Timer(3, lambda: os.kill(os.getpid(), SIGINT)).start()
    cmd = runner.invoke(
        cli,
        [
            "workflows",
            "run",
            "--workflow-id",
            workflow_result["id"],
            "--wait",
            "-1",
        ],
    )
    assert "Workflow run:" in cmd.output
    assert workflow_result["id"] in cmd.output
    assert "Cancellation request sent" in cmd.output
    assert "Workflow run complete: RUN_STATUS_CANCELLED" in cmd.output
    assert cmd.exit_code == 0
