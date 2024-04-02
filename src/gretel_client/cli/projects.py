from typing import Optional

import click

from gretel_client.cli.common import pass_session, SessionContext
from gretel_client.cli.hybrid import ClusterError, resolve_hybrid_environment
from gretel_client.config import RunnerMode, write_config
from gretel_client.projects import create_project
from gretel_client.projects.projects import get_project, search_projects


@click.group(help="Commands for working with Gretel projects.")
def projects(): ...


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
@click.option(
    "--project-type",
    metavar="PROJECT-TYPE",
    type=click.Choice(["hybrid", "cloud", "legacy"], case_sensitive=False),
    help=(
        "The project type to create. This will limit the mode in which models and workflows can be run. "
        "The default setting creates a 'legacy' project, supporting both hybrid and cloud workflows and models."
    ),
)
@click.option(
    "--hybrid-environment",
    metavar="ENVIRONMENT-ID-OR-NAME",
    help=(
        "(Hybrid projects only) the hybrid environment to use with the hybrid project. "
        "Run 'gretel hybrid environments list' to see a list of all available hybrid environments. "
        "Specify 'none' to create a hybrid project not bound to any hybrid environment (not recommended)."
    ),
)
@pass_session
def create(
    sc: SessionContext,
    name: str,
    desc: str,
    display_name: str,
    set_default: bool,
    project_type: Optional[str],
    hybrid_environment: Optional[str],
):
    if hybrid_environment:
        if not project_type:
            project_type = "hybrid"
            sc.log.info(
                "Creating a hybrid project due to a hybrid environment being specified."
            )
        elif project_type != "hybrid":
            raise click.BadOptionUsage(
                "--hybrid-environment",
                f"A hybrid environment can only be specified for hybrid, not {project_type} projects",
            )

    proj_runner_mode: Optional[RunnerMode] = None
    if project_type == "hybrid":
        proj_runner_mode = RunnerMode.HYBRID
    elif project_type == "cloud":
        proj_runner_mode = RunnerMode.CLOUD

    hybrid_cluster = None
    if project_type == "hybrid" and hybrid_environment != "none":
        try:
            hybrid_cluster = resolve_hybrid_environment(
                sc, hybrid_environment, allow_auto_select=True
            )
        except ClusterError as ex:
            if not hybrid_environment:
                raise ClusterError(
                    f"{str(ex)}\nIf you wish to create a hybrid project not bound to any hybrid environment "
                    "(not recommended), please specify 'none' as the hybrid environment."
                ) from ex
            raise

    if hybrid_cluster:
        if not hybrid_environment:
            sc.log.info(
                f"Automatically selected only available hybrid environment '{hybrid_cluster.name}'."
            )
        if sc.session.email != hybrid_cluster.owner_profile.email:
            sc.log.info(
                f"The deployment user of the hybrid environment, {hybrid_cluster.owner_profile.email}, will be granted "
                "administrator access to the new project."
            )

    project = create_project(
        name=name,
        desc=desc,
        display_name=display_name,
        session=sc.session,
        runner_mode=proj_runner_mode,
        hybrid_environment_guid=hybrid_cluster.guid if hybrid_cluster else None,
    )

    sc.log.info(f"Created project {project.name}.")
    sc.log.info(f"Console link: {project.get_console_url()}.")

    if set_default:
        sc.config.default_project_name = project.name
        write_config(sc.config)
        sc.log.info("Set project as default.")

    sc.print(data=project.info())

    # Issue a warning if the user has hybrid as their default runner mode,
    # but did not create a hybrid-only project (but only if they didn't specify
    # a project type explicitly).
    if (
        sc.session.default_runner == RunnerMode.HYBRID.value
        and project.runner_mode != RunnerMode.HYBRID
        and not project_type
    ):
        sc.log.warning(
            "Your default runner mode is set to 'hybrid', but the newly created "
            "project is a legacy project, supporting all runner modes."
        )
        sc.log.warning(
            "If you wish to create a project supporting hybrid models and workflows "
            "*only*, specify '--project-type hybrid' on project creation."
        )
    if project.runner_mode == RunnerMode.HYBRID and not project.cluster_guid:
        sc.log.warning(
            "Created a hybrid project not bound to any hybrid environment. "
            "This is not recommended."
        )
        sc.log.warning(
            "Please consult the documentation at "
            "https://docs.gretel.ai/operate-and-manage-gretel/gretel-hybrid for instructions "
            "on setting up a hybrid environment."
        )


@projects.command(help="Search for projects.")
@click.option("--limit", help="Limit the number of projects.", default=200)
@click.option("--query", help="Filter project names by a query string.", default=None)
@pass_session
def search(sc: SessionContext, limit: int, query: str):
    project_objs = search_projects(limit=limit, query=query, session=sc.session)
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
        project = get_project(name=name or uid, session=sc.session)
    else:
        raise click.BadOptionUsage("--name", "Please use --name or --uid option.")
    sc.print(data=project.as_dict)
    project.delete()
    sc.log.info("Project was deleted.")
