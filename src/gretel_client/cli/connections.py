import click

from gretel_client.cli.common import (
    parse_file,
    pass_session,
    project_option,
    SessionContext,
)
from gretel_client.config import get_session_config
from gretel_client.rest_v1.api.connections_api import ConnectionsApi
from gretel_client.rest_v1.models import CreateConnectionRequest


@click.group(
    help="Commands for working with Gretel connections.",
    hidden=not get_session_config().preview_features_enabled,
)
def connections():
    ...


def get_connections_api() -> ConnectionsApi:
    return get_session_config().get_v1_api(ConnectionsApi)


@connections.command(help="Create a new connection.")
@click.option(
    "--from-file",
    metavar="PATH",
    help="Path to the file containing Gretel connection.",
    required=True,
)
@project_option
@pass_session
def create(sc: SessionContext, from_file: str, project: str):
    connection_api = get_connections_api()
    conn = parse_file(from_file)

    # we try and configure a project in the following order
    #
    #  1. the project passed via `--project`.
    #  2. the project configured from the connection file.
    #  3. the default configured project from the system environment.
    #
    project_id = conn.get("project_id")

    # `project` is set if `--project` is passed.
    if project is not None:
        project_id = project

    # `project_id` is unset at this point if no project flag is passed, and no
    # project id is configured on the connection file.
    if project_id is None:
        project_id = sc.project.project_guid

    conn["project_id"] = sc.project.project_guid

    connection = connection_api.create_connection(CreateConnectionRequest(**conn))

    sc.log.info("Created connection:")
    sc.print(data=connection.to_dict())


@connections.command(help="Update a connection.")
@click.option(
    "--from-file", metavar="PATH", help="Path to the file containing Gretel connection."
)
@click.option(
    "--id", metavar="CONNECTION-ID", help="Gretel connection id.", required=True
)
@pass_session
def update(sc: SessionContext, id: str, from_file: str):
    connection_api = get_connections_api()
    conn = parse_file(from_file)
    connection = connection_api.update_connection(connection_id=id, connection=conn)

    sc.log.info("Updated connection:")
    sc.print(data=connection.to_dict())


@connections.command(help="Delete a connection.")
@click.option("--id", metavar="CONNECTION-ID", help="Gretel connection id.")
@pass_session
def delete(sc: SessionContext, id: str):
    sc.log.info(f"Deleting connection {id}.")
    connection_api = get_connections_api()
    connection_api.delete_connection(connection_id=id)

    sc.log.info(f"Deleted connection {id}.")


@connections.command(help="List connections.")
@pass_session
def list(sc: SessionContext):
    connection_api = get_connections_api()
    conns = connection_api.list_connections().data
    if not conns:
        sc.log.info("No connections found.")
        return
    sc.log.info("Connections:")
    for conn in conns:
        sc.print(data=conn.to_dict())


@connections.command(help="Get a connection.")
@click.option(
    "--id", metavar="CONNECTION-ID", help="Gretel connection id.", required=True
)
@pass_session
def get(sc: SessionContext, id: str):
    connection_api = get_connections_api()
    connection = connection_api.get_connection(connection_id=id)
    sc.log.info("Connection:")
    sc.print(data=connection.to_dict())
