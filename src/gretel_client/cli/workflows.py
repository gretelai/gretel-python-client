import sys
import time

from typing import Optional

import click
import yaml

from gretel_client.cli.common import pass_session, project_option, SessionContext
from gretel_client.config import get_session_config
from gretel_client.projects.common import WAIT_UNTIL_DONE
from gretel_client.rest_v1.api.workflows_api import (
    CancelWorkflowRunRequest,
    WorkflowsApi,
)
from gretel_client.rest_v1.model.workflow import Workflow
from gretel_client.rest_v1.model.workflow_run import WorkflowRun


@click.group(
    help="Commands for working with Gretel workflows.",
    hidden=not get_session_config().preview_features_enabled,
)
def workflows():
    ...


def get_workflows_api() -> WorkflowsApi:
    return get_session_config().get_v1_api(WorkflowsApi)


@workflows.command(help="Create a new workflow.")
@click.option("--name", metavar="NAME", help="Workflow name.")
@project_option
@click.option(
    "--config",
    metavar="PATH",
    help="Path to the file containing Gretel workflow config.",
    required=True,
)
@pass_session
def create(sc: SessionContext, config: str, name: str, project: str):

    with open(config, encoding="utf-8") as file:
        workflow_config = yaml.safe_load(file)

    if name:
        workflow_config["name"] = name

    project_id = sc.project.project_guid

    wfl = Workflow(
        config=workflow_config, name=workflow_config["name"], project_id=project_id
    )

    workflow_api = get_workflows_api()
    workflow = workflow_api.create_workflow(workflow=wfl)

    sc.log.info("Created workflow:")
    sc.print(data=workflow.to_dict())


@workflows.command(help="List workflows.")
@pass_session
def list(sc: SessionContext):
    workflow_api = get_workflows_api()
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
    workflow_api = get_workflows_api()
    workflow: Workflow = workflow_api.get_workflow(workflow_id=id)
    sc.log.info("Workflow:")
    sc.print(data=workflow.to_dict())


@workflows.command(help="Trigger a workflow.")
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
def trigger(sc: SessionContext, workflow_id: str, wait: int):
    workflow_api = get_workflows_api()
    workflow_run: WorkflowRun = workflow_api.create_workflow_run(
        workflow_run=WorkflowRun(workflow_id=workflow_id)
    )
    sc.log.info("Workflow run:")
    sc.print(data=workflow_run.to_dict())

    if wait == 0:
        return sc.log.info(
            "Parameter `--wait` 0 was specified, not waiting for the workflow run completion. The workflow run will remain running until it reaches the end state."
        )

    def cancel():
        sc.log.warning(
            "Interrupted, cancelling workflow run ...",
            prefix_nl=True,
        )
        workflow_api.cancel_workflow_run(
            workflow_run_id=workflow_run.id,
            cancel_workflow_run_request=CancelWorkflowRunRequest(workflow_run.id),
        )

        sc.log.warning(
            "Cancellation request sent, waiting for workflow to reach CANCELLED state. Send another interrupt to exit immediately.",
            prefix_nl=True,
        )
        _wait_for_workflow_completion(sc, workflow_run.id, -1)

    sc.register_cleanup(cancel)

    res = _wait_for_workflow_completion(sc, workflow_run.id, wait)
    if res is None:
        sc.log.warning(
            f"Workflow run hasn't completed after waiting for {wait} seconds. Exiting the script, but the workflow run will remain running until it reaches the end state.",
            prefix_nl=True,
        )
        sc.exit(0)
    sc.exit(0 if res else 1)


def _wait_for_workflow_completion(
    sc: SessionContext, workflow_run_id: str, wait: int
) -> Optional[bool]:
    workflow_api = get_workflows_api()

    start = time.time()
    i = 0
    statuses = WorkflowRun.allowed_values[("status",)]
    final_statuses = {
        statuses["ERROR"],
        statuses["LOST"],
        statuses["COMPLETED"],
        statuses["CANCELLED"],
    }
    while True:
        if wait >= 0 and time.time() - start > wait:
            return None

        workflow_run: WorkflowRun = workflow_api.get_workflow_run(
            workflow_run_id=workflow_run_id
        )
        if i % 5 == 0:
            i = 0
            workflow_run = workflow_api.get_workflow_run(
                workflow_run_id=workflow_run_id
            )
            print("\r\033[K", end="", flush=True, file=sys.stderr)
            sc.log.info(f"Workflow status is: {workflow_run.status}", nl=False)

            if workflow_run.status in final_statuses:
                sc.log.info(
                    f"Workflow run complete: {workflow_run.status}", prefix_nl=True
                )
                return workflow_run.status == statuses["COMPLETED"]

        print(".", end="", flush=True, file=sys.stderr)

        i += 1
        time.sleep(1)
