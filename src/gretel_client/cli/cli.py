import click

from gretel_client.cli.agent import agent
from gretel_client.cli.artifacts import artifacts
from gretel_client.cli.common import pass_session, SessionContext
from gretel_client.cli.connections import connections
from gretel_client.cli.errors import ExceptionHandler
from gretel_client.cli.hybrid import hybrid
from gretel_client.cli.models import models
from gretel_client.cli.projects import projects
from gretel_client.cli.records import records
from gretel_client.cli.workflows import workflows
from gretel_client.config import (
    ClientConfig,
    configure_session,
    get_session_config,
    RunnerMode,
    write_config,
)
from gretel_client.projects.common import f
from gretel_client.users import users


class GretelCliHandler(ExceptionHandler): ...


@click.group(cls=GretelCliHandler)
@click.option("--debug/--no-debug", default=False, help="Show extra debug messages.")
@click.option(
    "--output",
    type=click.Choice(["auto", "json"], case_sensitive=False),
    default="auto",
    help="Control how output data is formatted.",
)
@click.pass_context
def cli(ctx: click.Context, debug: bool, output: str):
    """The Gretel CLI."""
    ctx.obj = SessionContext(ctx, output_fmt=output, debug=debug)


def _check_endpoint(endpoint: str) -> str:
    endpoint = endpoint.strip("/")
    if not endpoint.startswith("https://"):
        endpoint = f"https://{endpoint.split('/')[-1]}"
    return endpoint


@cli.command(help="Configures the Gretel CLI.")
@click.option(
    "--endpoint",
    prompt="Endpoint",
    default=lambda: get_session_config().endpoint,
    metavar="URL",
    help="Gretel API endpoint.",
)
@click.option(
    "--artifact-endpoint",
    prompt="Artifact Endpoint",
    default=lambda: get_session_config().artifact_endpoint,
    metavar="URL",
    help="Specify the endpoint for project and model artifacts.",
)
@click.option(
    "--default-runner",
    prompt="Default Runner",
    default=lambda: get_session_config().default_runner,
    type=click.Choice(
        [RunnerMode.CLOUD.value, RunnerMode.LOCAL.value, RunnerMode.HYBRID.value],
        case_sensitive=False,
    ),
    metavar="RUNNER",
    help="Specify the default runner.",
)
@click.option(
    "--api-key",
    prompt="Gretel API Key",
    hide_input=True,
    default=lambda: get_session_config().masked_api_key,
    metavar="API",
    help="Gretel API key.",
)
@click.option(
    "--project",
    prompt="Default Project",
    default=lambda: get_session_config().default_project_name or "none",
    metavar="PROJECT",
    help="Default Gretel project.",
)
@pass_session
def configure(
    sc: SessionContext,
    endpoint: str,
    artifact_endpoint: str,
    api_key: str,
    project: str,
    default_runner: str,
):
    project_name = None if project == "none" else project
    endpoint = _check_endpoint(endpoint)
    # swap the api key back to the original if one was
    # already read in from the config and not updated
    if api_key.endswith("****"):
        api_key = sc.session.api_key
    config = ClientConfig(
        endpoint=endpoint,
        artifact_endpoint=artifact_endpoint,
        api_key=api_key,
        default_runner=default_runner,
    )
    config.update_default_project(project_id=project_name)
    configure_session(config)

    config_path = write_config(config)
    sc.log.info(f"Configuration written to {config_path}. Done.")

    sc.print(data=config.masked)


@cli.command(help="Check account, user and configuration details.")
@pass_session
def whoami(sc: SessionContext):
    me = users.get_me(session=sc.session)
    sc.print(data={f.EMAIL: me[f.EMAIL], "config": sc.config.masked})


cli.add_command(models)
cli.add_command(records)
cli.add_command(projects)
cli.add_command(artifacts)
cli.add_command(connections)
cli.add_command(workflows)
cli.add_command(hybrid)
cli.add_command(agent)


if __name__ == "__main__":
    cli()
