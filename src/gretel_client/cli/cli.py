import click

from gretel_client.cli.artifacts import artifacts
from gretel_client.cli.common import pass_session, SessionContext
from gretel_client.cli.models import models
from gretel_client.cli.projects import projects
from gretel_client.cli.records import records
from gretel_client.config import (
    ClientConfig,
    configure_session,
    get_session_config,
    GretelClientConfigurationError,
    RunnerMode,
    write_config,
)


@click.group()
@click.option("--debug/--no-debug", default=False, help="Show extra debug messages.")
@click.option(
    "--output",
    type=click.Choice(["json"], case_sensitive=False),
    default="json",
)
@click.pass_context
def cli(ctx: click.Context, debug: bool, output: str):
    """The Gretel CLI."""
    ctx.obj = SessionContext(ctx, output_fmt=output, debug=debug)


@cli.command(help="Configures the Gretel CLI.")
@click.option(
    "--endpoint",
    prompt="Endpoint",
    default=lambda: get_session_config().endpoint,
    metavar="URL",
    help="Gretel API endpoint.",
)
@click.option(
    "--default-runner",
    prompt="Default Runner",
    default=lambda: get_session_config().default_runner,
    type=click.Choice(
        [RunnerMode.CLOUD.value, RunnerMode.LOCAL.value], case_sensitive=False
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
    sc: SessionContext, endpoint: str, api_key: str, project: str, default_runner: str
):
    project_name = None if project == "none" else project

    # swap the api key back to the original if one was
    # already read in from the config and not updated
    if api_key.endswith("****"):
        api_key = get_session_config().api_key
    config = ClientConfig(
        endpoint=endpoint, api_key=api_key, default_runner=default_runner
    )

    try:
        config.update_default_project(project_id=project_name)
    except GretelClientConfigurationError as ex:
        sc.log.error(f"The project {project_name} is invalid", ex=ex)
        sc.exit(1)

    configure_session(config)

    try:
        config_path = write_config(config)
        sc.log.info(f"Configuration written to {config_path}. Done.")
    except Exception as ex:
        sc.log.error("Could not write configuration to.", ex=ex)

    sc.print(data=config.masked)


cli.add_command(models)
cli.add_command(records)
cli.add_command(projects)
cli.add_command(artifacts)


if __name__ == "__main__":
    cli()
