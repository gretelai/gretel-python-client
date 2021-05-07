import click

from gretel_client_v2.config import (
    get_session_config,
    GRETEL,
    DEFAULT_GRETEL_ENDPOINT,
    write_config,
    _ClientConfig,
)
from gretel_client_v2._cli.common import (
    SessionContext,
    validate_project,
    pass_session
)
from gretel_client_v2._cli.models import models
from gretel_client_v2._cli.projects import projects
from gretel_client_v2._cli.records import records


@click.group()
@click.option("--debug/--no-debug", default=False)
@click.option(
    "--output",
    type=click.Choice(["json"], case_sensitive=False),
    default="json",
)
@click.pass_context
def cli(ctx: click.Context, debug: bool, output: str):
    ctx.obj = SessionContext(ctx, output_fmt=output, debug=debug)


@cli.command()
@click.option("--endpoint", prompt="Endpoint", default=DEFAULT_GRETEL_ENDPOINT)
@click.option("--api-key", prompt="Gretel API Key", hide_input=True)
@click.option("--project", prompt="Default Project", default="none")
@pass_session
def configure(sc: SessionContext, endpoint: str, api_key: str, project: str):

    project_name = None if project == "none" else project
    if project_name and not validate_project(project_name):
        sc.log.error(f"Project {project_name} not valid.")
        sc.exit(1)

    config = _ClientConfig(
        endpoint=endpoint, api_key=api_key, default_project_name=project_name
    )

    try:
        config_path = write_config(config)
        sc.log.info(f"Configuration written to {config_path}. Done.")
    except Exception as ex:
        sc.log.debug(ex)
        sc.log.error("There was a problem configuring the Gretel CLI tool.")

    sc.print(data=get_session_config().masked)


cli.add_command(models)
cli.add_command(records)
cli.add_command(projects)


if __name__ == "__main__":
    cli(auto_env_prefix=GRETEL)
