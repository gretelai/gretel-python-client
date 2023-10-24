import base64
import json

from json import JSONDecodeError
from pathlib import Path
from typing import Dict, Optional

import click
import yaml

from gretel_client.cli.common import pass_session, SessionContext
from gretel_client.cli.connection_credentials import aws_kms_flags, AWSKMSEncryption
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


def _read_connection_file(file: str) -> dict:
    fp = Path(file).resolve()
    try:
        with open(fp) as fd:
            return json.load(fd)
    except JSONDecodeError:
        with open(fp) as fd:
            return yaml.safe_load(fd)


@connections.command(help="Create a new connection.")
@click.option(
    "--from-file",
    metavar="PATH",
    help="Path to the file containing Gretel connection.",
    required=True,
)
@click.option(
    "--project",
    metavar="PROJECT-ID",
    help="Specify the project to create the connection in.",
    required=False,
)
@aws_kms_flags("aws_kms")
@pass_session
def create(
    sc: SessionContext,
    from_file: str,
    project: Optional[str],
    aws_kms: Optional[AWSKMSEncryption],
):
    connection_api = get_connections_api()

    conn = _read_connection_file(from_file)

    # we try and configure a project in the following order
    #
    #  1. the project passed via `--project`.
    #  2. the project configured from the connection file.
    #  3. the default configured project from the system environment.
    #
    project_id = conn.get("project_id")

    # `project` is set if `--project` is passed.
    if project:
        if project_id and project_id != project:
            sc.log.warning(
                f"Overriding project {project_id} in connections config file with project {project} specified on the command line",
            )
        project_id = project

    # `project_id` is unset at this point if no project flag is passed, and no
    # project id is configured on the connection file.
    if not project_id:
        project_id = sc.project.project_guid

    conn["project_id"] = project_id

    # Add more supported encryption providers here, if applicable
    encryption_providers = [p for p in (aws_kms,) if p is not None]
    if len(encryption_providers) > 1:
        raise click.UsageError(
            "Please specify options for at most one encryption provider only"
        )
    encryption_provider = encryption_providers[0] if encryption_providers else None

    if conn.get("encrypted_credentials") is not None:
        if conn.get("credentials") is not None:
            raise ValueError(
                "connection config must not specify both encrypted and plaintext credentials"
            )
        if encryption_provider is not None:
            raise click.UsageError(
                "Encryption provider options must not be used if connection config contains pre-encrypted "
                "credentials",
            )
    elif encryption_provider is not None:
        conn["encrypted_credentials"] = encryption_provider.apply(
            conn.pop("credentials", None)
        )
    elif "credentials" not in conn:
        raise ValueError(
            "connection config contains neither plaintext nor encrypted credentials, "
            "and you have not specified pre-encrypted credentials either."
        )

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
    conn = _read_connection_file(from_file)
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
