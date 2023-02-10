import click
import yaml

from gretel_client.cli.common import pass_session, project_option, SessionContext
from gretel_client.config import get_session_config
from gretel_client.rest_v1.api.workflows_api import WorkflowsApi
from gretel_client.rest_v1.model.workflow import Workflow


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

    wfl = Workflow(config=workflow_config, project_id=project_id)

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
