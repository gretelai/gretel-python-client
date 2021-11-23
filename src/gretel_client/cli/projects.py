import click

from gretel_client.cli.common import pass_session, SessionContext
from gretel_client.config import write_config
from gretel_client.projects import create_project
from gretel_client.projects.projects import search_projects


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
    project = create_project(name=name, desc=desc, display_name=display_name)

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
    sc.config.update_default_project(name)
    write_config(sc.config)
    sc.log.info(f"Set {name} as the default project.")
    sc.print(data=sc.config.masked)
