import click

from gretel_client.cli.common import parse_file, pass_session, SessionContext
from gretel_client.config import get_session_config
from gretel_client.rest_v1.api.connections_api import ConnectionsApi
from gretel_client.rest_v1.model.connection import Connection


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
@pass_session
def create(sc: SessionContext, from_file: str):
    connection_api = get_connections_api()
    conn = parse_file(from_file)
    connection = connection_api.create_connection(connection=conn)

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
    connection: Connection = connection_api.get_connection(connection_id=id)
    sc.log.info("Connection:")
    sc.print(data=connection.to_dict())
