import click

from gretel_client.cli.common import pass_session, SessionContext
from gretel_client.config import write_config
from gretel_client.projects import create_project
from gretel_client.projects.projects import get_project, search_projects


@click.group(help="Commands for working with Gretel projects.")
def projects():
    ...


@projects.command(help="Create a new project.")
@click.option("--name", metavar="NAME", help="Gretel project name.")
@click.option("--desc", metavar="DESCRIPTION", help="Gretel project description.")
@click.option(
    "--display-name", metavar="DISPLAY-NAME", help="This will show on the console."
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
    sc.log.info(f"Console link: {project.get_console_url()}.")

    if set_default:
        sc.config.default_project_name = project.name
        write_config(sc.config)
        sc.log.info("Set project as default.")

    sc.print(data=project.info())


@projects.command(help="Search for projects.")
@click.option("--limit", help="Limit the number of projects.", default=200)
@click.option("--query", help="Filter project names by a query string.", default=None)
@pass_session
def search(sc: SessionContext, limit: int, query: str):
    project_objs = search_projects(limit=limit, query=query)
    projects_table = [p.as_dict for p in project_objs]
    sc.print(data=projects_table)


@projects.command(help="Set default project.")
@click.option(
    "--name",
    metavar="PROJECT-NAME",
    help="Project name to set as default.",
    required=True,
)
@pass_session
def set_default(sc: SessionContext, name: str):
    sc.config.update_default_project(name)
    write_config(sc.config)
    sc.log.info(f"Set {name} as the default project.")
    sc.print(data=sc.config.masked)


@projects.command(help="Delete project.")
@click.option(
    "--name",
    metavar="PROJECT-NAME",
    help="Gretel project name, mutually exclusive with id.",
    default=None,
)
@click.option(
    "--uid",
    metavar="PROJECT-ID",
    help="Gretel project id, mutually exclusive with name.",
    default=None,
)
@pass_session
def delete(sc: SessionContext, name: str, uid: str):
    if name and uid:
        raise click.BadOptionUsage(
            "--uid",
            f"Cannot pass both --uid and --name. Please use --name or --uid option.",
        )
    if name or uid:
        project = get_project(name=name or uid)
    else:
        raise click.BadOptionUsage("--name", "Please use --name or --uid option.")
    sc.print(data=project.as_dict)
    project.delete()
    sc.log.info("Project was deleted.")
