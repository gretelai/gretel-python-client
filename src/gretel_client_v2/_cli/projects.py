import click

from gretel_client_v2._cli.common import (
    SessionContext,
    validate_project,
    pass_session
)
from gretel_client_v2.projects.projects import search_projects
from gretel_client_v2.projects import get_project
from gretel_client_v2.rest.exceptions import NotFoundException, UnauthorizedException
from gretel_client_v2.config import write_config


@click.group()
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
def create(sc: SessionContext, name: str, desc: str, display_name: str, set_default: bool):
    project = None
    try:
        project = get_project(
            name=name, desc=desc, display_name=display_name, create=True
        )
    except (UnauthorizedException, NotFoundException) as ex:
        sc.log.debug(ex)
        sc.log.error(f"Project name {name} unavailable.")

    if not project:
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
    projects = search_projects(limit=limit, query=query)
    projects_table = [p.as_dict for p in projects]
    sc.print(data=projects_table)


@projects.command()
@click.option(
    "--name",
    metavar="project-name",
    help="Project name to set as default.",
    required=True,
)
@pass_session
def set_default(sc: SessionContext, name: str):
    if not validate_project(name):
        sc.log.error(f"Project {name} does not exist.")
        sc.exit(1)

    sc.config.default_project_name = name
    write_config(sc.config)

    sc.log.info(f"Set {name} as the default project.")
    sc.print(data=sc.config.masked)
