import json

from json import JSONDecodeError
from pathlib import Path
from typing import Optional

import click
import yaml

from gretel_client.cli.common import pass_session, project_option, SessionContext
from gretel_client.cli.connection_credentials_aws_kms import AWSKMSEncryption
from gretel_client.cli.connection_credentials_azure_key_vault import (
    AzureKeyVaultEncryption,
)
from gretel_client.cli.connection_credentials_gcp_kms import GCPKMSEncryption
from gretel_client.config import ClientConfig, get_session_config
from gretel_client.rest_v1.api.connections_api import ConnectionsApi
from gretel_client.rest_v1.models import (
    CreateConnectionRequest,
    UpdateConnectionRequest,
)


@click.group(help="Commands for working with Gretel connections.")
def connections():
    ...


def _get_connections_api(*, session: ClientConfig) -> ConnectionsApi:
    return session.get_v1_api(ConnectionsApi)


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
@AWSKMSEncryption.options("aws_kms")
@GCPKMSEncryption.options("gcp_kms")
@AzureKeyVaultEncryption.options("azure_key_vault")
@pass_session
@project_option
def create(
    sc: SessionContext,
    from_file: str,
    project: Optional[str],
    aws_kms: Optional[AWSKMSEncryption],
    gcp_kms: Optional[GCPKMSEncryption],
    azure_key_vault: Optional[AzureKeyVaultEncryption],
):
    connection_api = _get_connections_api(session=sc.session)

    conn = _read_connection_file(from_file)

    # we try and configure a project in the following order
    #
    #  1. the project passed via `--project` or from the system environment (CLI takes precedence)
    #  2. the default configured project from the system environment.
    #
    project_guid = conn.get("project_id")

    if project or not project_guid:
        if project_guid is not None and sc.project.project_guid != project_guid:
            sc.log.warning(
                f"Overriding project {project_guid} in connections config file with project {sc.project.project_guid}.",
            )
        project_guid = sc.project.project_guid

    conn["project_id"] = project_guid

    # Add more supported encryption providers here, if applicable
    encryption_providers = [
        p for p in (aws_kms, gcp_kms, azure_key_vault) if p is not None
    ]
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
    connection_api = _get_connections_api(session=sc.session)
    conn = _read_connection_file(from_file)
    existing_connection = connection_api.get_connection(connection_id=id)
    if existing_connection.type.lower() != conn["type"].lower():
        raise ValueError(
            f"cannot change type of connect {id} from '{existing_connection.type}' to '{conn['type']}'"
        )
    del conn["type"]
    update_conn_req = UpdateConnectionRequest(**conn)
    connection = connection_api.update_connection(
        connection_id=id,
        update_connection_request=update_conn_req,
    )

    sc.log.info("Updated connection:")
    sc.print(data=connection.to_dict())


@connections.command(help="Delete a connection.")
@click.option("--id", metavar="CONNECTION-ID", help="Gretel connection id.")
@pass_session
def delete(sc: SessionContext, id: str):
    sc.log.info(f"Deleting connection {id}.")
    connection_api = _get_connections_api(session=sc.session)
    connection_api.delete_connection(connection_id=id)

    sc.log.info(f"Deleted connection {id}.")


@connections.command(help="List connections.")
@pass_session
def list(sc: SessionContext):
    connection_api = _get_connections_api(session=sc.session)
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
    connection_api = _get_connections_api(session=sc.session)
    connection = connection_api.get_connection(connection_id=id)
    sc.log.info("Connection:")
    sc.print(data=connection.to_dict())
