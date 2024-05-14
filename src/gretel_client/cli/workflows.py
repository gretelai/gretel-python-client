import json
import sys
import time

from pathlib import Path
from typing import Optional

import click
import yaml

from gretel_client.cli.common import pass_session, project_option, SessionContext
from gretel_client.config import ClientConfig, get_session_config
from gretel_client.projects.common import WAIT_UNTIL_DONE
from gretel_client.rest_v1.api.workflows_api import WorkflowsApi
from gretel_client.rest_v1.models import (
    CreateWorkflowRequest,
    CreateWorkflowRunRequest,
    Workflow,
    WorkflowRun,
)
from gretel_client.workflows.runner_mode import RunnerMode
from gretel_client.workflows.status import Status, TERMINAL_STATES


@click.group(
    help="Commands for working with Gretel workflows.",
)
def workflows(): ...


def _get_workflows_api(*, session: ClientConfig) -> WorkflowsApi:
    return session.get_v1_api(WorkflowsApi)


def _determine_runner_mode() -> str:
    default_runner = get_session_config().default_runner
    if not default_runner:
        return RunnerMode.RUNNER_MODE_CLOUD
    return RunnerMode.from_str(default_runner)


@workflows.command(help="Create a new workflow.")
@click.option("--name", metavar="NAME", help="Workflow name.")
@project_option
@click.option(
    "--config",
    metavar="PATH",
    help="Path to the file containing Gretel workflow config.",
    required=True,
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, path_type=Path
    ),
)
@click.option(
    "--runner_mode",
    metavar="NAME",
    help="The RunnerMode to use by default when running this workflow.",
    default=_determine_runner_mode,
)
@pass_session
def create(
    sc: SessionContext,
    config: Path,
    name: str,
    project: str,
    runner_mode: str = RunnerMode.RUNNER_MODE_CLOUD.value,
):
    with open(config, encoding="utf-8") as file:
        workflow_config = yaml.safe_load(file)

    if name:
        workflow_config["name"] = name

    project_id = sc.project.project_guid
    runner_mode = RunnerMode.from_str(runner_mode)

    wfl = CreateWorkflowRequest(
        config_text=yaml.dump(workflow_config, sort_keys=False),
        name=workflow_config["name"],
        project_id=project_id,
        runner_mode=runner_mode,
    )

    workflow_api = _get_workflows_api(session=sc.session)
    workflow = workflow_api.create_workflow(wfl)

    sc.log.info("Created workflow:")
    sc.print(data=workflow.to_dict())


@workflows.command(help="List workflows.")
@pass_session
def list(sc: SessionContext):
    workflow_api = _get_workflows_api(session=sc.session)
    wfls = workflow_api.get_workflows().workflows
    if not wfls:
        sc.log.info("No workflows found.")
        return
    sc.log.info("Workflows:")
    for workflow in wfls:
        sc.print(data=workflow.to_dict())


@workflows.command(help="Get a workflow.")
@click.option(
    "--id",
    metavar="WORKFLOW-ID",
    help="Gretel workflow id.",
    required=True,
)
@pass_session
def get(sc: SessionContext, id: str):
    workflow_api = _get_workflows_api(session=sc.session)
    workflow: Workflow = workflow_api.get_workflow(workflow_id=id)
    sc.log.info("Workflow:")
    sc.print(data=workflow.to_dict())


@workflows.command(help="Update a workflow with a config.")
@click.option(
    "--workflow-id",
    metavar="WORKFLOW-ID",
    help="Gretel workflow id.",
    required=True,
)
@click.option(
    "--config",
    metavar="PATH",
    help="Path to the file containing Gretel workflow config.",
    required=True,
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, path_type=Path
    ),
)
@pass_session
def update(sc: SessionContext, workflow_id: str, config: Path):
    with open(config, encoding="utf-8") as file:
        workflow_config = file.read()
    workflow_api = _get_workflows_api(session=sc.session)
    updated_workflow = workflow_api.update_workflow_config(
        workflow_id=workflow_id, body=workflow_config, _content_type="text/yaml"
    )
    sc.log.info("Updated workflow:")
    sc.print(data=updated_workflow.to_dict())


@workflows.command(help="Run a workflow.")
@click.option(
    "--wait",
    metavar="SECONDS",
    help="Wait for workflow run to complete.",
    default=WAIT_UNTIL_DONE,
)
@click.option(
    "--workflow-id",
    metavar="WORKFLOW-ID",
    help="Gretel workflow id.",
    required=True,
)
@pass_session
def run(sc: SessionContext, workflow_id: str, wait: int):
    workflow_api = _get_workflows_api(session=sc.session)
    workflow_run: WorkflowRun = workflow_api.create_workflow_run(
        CreateWorkflowRunRequest(workflow_id=workflow_id)
    )
    sc.log.info("Workflow run:")
    sc.print(data=workflow_run.to_dict())

    if wait == 0:
        return sc.log.info(
            "Parameter `--wait` 0 was specified, not waiting for the workflow run completion. The workflow run will "
            "remain running until it reaches the end state."
        )

    def cancel():
        sc.log.warning(
            "Interrupted, cancelling workflow run ...",
            prefix_nl=True,
        )
        workflow_api.cancel_workflow_run(workflow_run_id=workflow_run.id)

        sc.log.warning(
            "Cancellation request sent, waiting for workflow to reach CANCELLED state. Send another interrupt to exit "
            "immediately.",
            prefix_nl=True,
        )
        _wait_for_workflow_completion(sc, workflow_run.id, -1)

    sc.register_cleanup(cancel)

    res = _wait_for_workflow_completion(sc, workflow_run.id, wait)
    if res is None:
        sc.log.warning(
            f"Workflow run hasn't completed after waiting for {wait} seconds. Exiting the script, but the workflow "
            f"run will remain running until it reaches the end state.",
            prefix_nl=True,
        )
        sc.exit(0)
    sc.exit(0 if res else 1)


def _wait_for_workflow_completion(
    sc: SessionContext, workflow_run_id: str, wait: int
) -> Optional[bool]:
    workflow_api = _get_workflows_api(session=sc.session)

    start = time.time()
    i = 0
    while True:
        if 0 <= wait < time.time() - start:
            return None

        if i % 5 == 0:
            i = 0
            workflow_run = workflow_api.get_workflow_run(
                workflow_run_id=workflow_run_id
            )
            print("\r\033[K", end="", flush=True, file=sys.stderr)
            sc.log.info(f"Workflow status is: {workflow_run.status}", nl=False)

            if workflow_run.status in TERMINAL_STATES:
                sc.log.info(
                    f"Workflow run complete: {workflow_run.status}", prefix_nl=True
                )
                return workflow_run.status == Status.RUN_STATUS_COMPLETED

        print(".", end="", flush=True, file=sys.stderr)

        i += 1
        time.sleep(1)
