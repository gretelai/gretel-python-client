import click
import json

from gretel_client.cli.common import SessionContext, pass_session
from gretel_client.projects.projects import search_projects
from gretel_client.projects import create_project
from gretel_client.rest.exceptions import ApiException, UnauthorizedException
from gretel_client.config import GretelClientConfigurationError, write_config


@click.group(help="Commands for working with Gretel projects.")
def projects():
    ...


@projects.command()
@click.option("--name", metavar="name", help="Gretel project name.")
@click.option("--desc", metavar="description", help="Gretel project description.")
@click.option(
    "--display-name", metavar="display-name", help="This will show on the console."
)
@click.option(
    "--set-default",
    is_flag=True,
    default=False,
    help="Use this project as the default.",
)
@pass_session
def create(
    sc: SessionContext, name: str, desc: str, display_name: str, set_default: bool
):
    try:
        project = create_project(
            name=name, desc=desc, display_name=display_name
        )
    except ApiException as ex:
        if ex.status == 400:
            error_dict = json.loads(ex.body)
            error_msg = error_dict.get("message")
            sc.log.error(
                (
                    f"Error creating project: {error_msg}"
                ),
                ex=ex,
            )
            sc.exit(1)
        elif ex.status == 401:
            sc.log.error("Unauthorized. Please check your credentials.")
            sc.exit(1)
        else:
            sc.log.error(f"HTTP Error: {ex.body}")
            sc.exit(1)

    sc.log.info(f"Created project {project.name}.")
    sc.log.info(f"Console link: {project.get_console_url()}")

    if set_default:
        sc.config.default_project_name = project.name
        write_config(sc.config)
        sc.log.info("Set project as default.")

    sc.print(data=project.info())


@projects.command()
@click.option("--limit", help="Limit the number of projects.", default=200)
@click.option("--query", help="Filter project names by a query string.", default=None)
@pass_session
def search(sc: SessionContext, limit: int, query: str):
    try:
        projects = search_projects(limit=limit, query=query)
        projects_table = [p.as_dict for p in projects]
        sc.print(data=projects_table)
    except UnauthorizedException:
        sc.log.error("Unauthorized. Please check your credentials.")
        sc.exit(1)


@projects.command()
@click.option(
    "--name",
    metavar="project-name",
    help="Project name to set as default.",
    required=True,
)
@pass_session
def set_default(sc: SessionContext, name: str):
    try:
        sc.config.update_default_project(name)
    except GretelClientConfigurationError as ex:
        sc.log.error(f"Project {name} does not exist.", ex=ex)
        sc.exit(1)

    write_config(sc.config)

    sc.log.info(f"Set {name} as the default project.")
    sc.print(data=sc.config.masked)
