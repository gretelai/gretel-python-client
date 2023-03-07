import sys
import time

import click
import yaml

from gretel_client.cli.common import pass_session, project_option, SessionContext
from gretel_client.config import get_session_config
from gretel_client.projects.common import WAIT_UNTIL_DONE
from gretel_client.rest_v1.api.workflows_api import WorkflowsApi
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
    start = time.time()
    i = 0
    statuses = WorkflowRun.allowed_values[("status",)]
    final_statuses = {statuses["ERROR"], statuses["LOST"], statuses["COMPLETED"]}
    while True:
        if wait >= 0 and time.time() - start > wait:
            sc.log.warning(
                f"Workflow run hasn't completed after waiting for {wait} seconds. Exiting the script, but the workflow run will remain running until it reaches the end state.",
                prefix_nl=True,
            )
            sc.exit(0)

        workflow_run: WorkflowRun = workflow_api.get_workflow_run(
            workflow_run_id=workflow_run.id
        )
        if i % 5 == 0:
            i = 0
            workflow_run = workflow_api.get_workflow_run(
                workflow_run_id=workflow_run.id
            )
            print("\r\033[K", end="", flush=True, file=sys.stderr)
            sc.log.info(f"Workflow status is: {workflow_run.status}", nl=False)

            if workflow_run.status in final_statuses:
                sc.log.info(
                    f"Workflow run complete: {workflow_run.status}", prefix_nl=True
                )
                if workflow_run.status == statuses["COMPLETED"]:
                    sc.exit(0)
                else:
                    sc.exit(1)

        print(".", end="", flush=True, file=sys.stderr)

        i += 1
        time.sleep(1)
