import click

from gretel_client.config import (
    GretelClientConfigurationError,
    configure_session,
    get_session_config,
    write_config,
    ClientConfig,
)
from gretel_client.cli.common import SessionContext, pass_session
from gretel_client.cli.models import models
from gretel_client.cli.projects import projects
from gretel_client.cli.records import records
from gretel_client.cli.artifacts import artifacts


@click.group()
@click.option("--debug/--no-debug", default=False, help="Show extra debug messages.")
@click.option(
    "--output",
    type=click.Choice(["json"], case_sensitive=False),
    default="json",
)
@click.pass_context
def cli(ctx: click.Context, debug: bool, output: str):
    """The Gretel CLI.
    """
    ctx.obj = SessionContext(ctx, output_fmt=output, debug=debug)


_copyright_data = """
The Gretel CLI and Python SDK, installed through the "gretel-client" 
package or other mechanism is free and open source software under
the Apache 2.0 License.

When using the CLI or SDK, you may launch "Gretel Worker(s)"
that are hosted in your local environment as containers. These
workers are launched automatically when running commands that create
models or process data records.

The "Gretel Worker" and all code within it is copyrighted and an
extension of the Gretel Service and licensed under the Gretel.ai
Terms of Service.  These terms can be found at https://gretel.ai/terms
section G paragraph 2.
"""


def copyright(fn):
    click.echo(click.style("\nGretel.ai COPYRIGHT Notice\n", fg="yellow"))
    click.echo(_copyright_data + "\n\n")
    return fn


_current_config = get_session_config()


def _set_api_key() -> str:
    if not _current_config.api_key:
        return "None"

    return _current_config.api_key[:8] + "****"


@cli.command(help="Configures the Gretel CLI.")
@copyright
@click.option(
    "--endpoint",
    prompt="Endpoint",
    default=_current_config.endpoint,
    metavar="URL",
    help="Gretel API endpoint.",
)
@click.option(
    "--default-runner",
    prompt="Default Runner",
    default=_current_config.default_runner,
    type=click.Choice(["local"], case_sensitive=False),
    metavar="RUNNER",
    help="Specify the default runner.",
)
@click.option(
    "--api-key",
    prompt="Gretel API Key",
    hide_input=True,
    default=_set_api_key(),
    metavar="API",
    help="Gretel API key.",
)
@click.option(
    "--project",
    prompt="Default Project",
    default=_current_config.default_project_name or "None",
    metavar="PROJECT",
    help="Default Gretel project.",
)
@pass_session
def configure(
    sc: SessionContext, endpoint: str, api_key: str, project: str, default_runner: str
):
    project_name = None if project == "none" else project
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
