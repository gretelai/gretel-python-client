import base64
import json
import os

from pathlib import Path
from typing import Callable

import pytest

from click.testing import CliRunner

from gretel_client.cli.cli import cli
from gretel_client.cli.connections import _read_connection_file
from gretel_client.projects.projects import Project
from gretel_client.rest_v1.api.connections_api import ConnectionsApi

GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME = os.getenv(
    "GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME"
)


def _parse_output(output: str) -> dict:
    return json.loads(output.rsplit("Created connection:\n")[1])


@pytest.mark.skipif(
    not os.getenv("GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME"),
    reason="Skipping hybrid tests based on missing GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME environment variable",
)
def test_gcp_hybrid_connection_crud_from_cli(
    get_fixture: Callable,
    project: Project,
    connections_api: ConnectionsApi,
):
    assert (
        GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME
    ), "GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME must be set for this test"

    runner = CliRunner()

    # Only pass an encrypted credentials file, no key RN
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection.json"),
            "--gcp-kms-encrypted-credentials",
            get_fixture("connections/test_encrypted_creds_gcp.b64"),
        ],
    )
    assert "--gcp-kms-key-resource-name" in cmd.output
    assert cmd.exit_code != 0

    # Encrypt the credentials in the connection config
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection.json"),
            "--gcp-kms-key-resource-name",
            GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME,
        ],
    )
    assert "Created connection:" in cmd.output
    assert cmd.exit_code == 0
    connection_result = _parse_output(cmd.output)

    conn_with_creds = connections_api.get_connection_with_credentials(
        connection_result["id"]
    )
    assert (
        conn_with_creds.encrypted_credentials["gcp_kms"]["resource_name"]
        == GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME
    )

    # Attempt to use a pre-encrypted credentials file with a connection config that
    # contains plaintext credentials
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection.json"),
            "--gcp-kms-key-resource-name",
            GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME,
            "--gcp-kms-encrypted-credentials",
            get_fixture("connections/test_encrypted_creds_gcp.b64"),
        ],
    )
    assert "An encrypted credentials file must not be specified" in cmd.output
    assert cmd.exit_code != 0

    # Attempt to use a connection config without plaintext credentials without a
    # pre-encrypted credentials file.
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection_without_creds.json"),
            "--gcp-kms-key-resource-name",
            GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME,
        ],
    )
    assert "An encrypted credentials file must be specified" in cmd.output
    assert cmd.exit_code != 0

    # Create a connection with a connection config without plaintext credentials
    # and a pre-encrypted credentials file (b64 encoded).
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection_without_creds.json"),
            "--gcp-kms-key-resource-name",
            GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME,
            "--gcp-kms-encrypted-credentials",
            get_fixture("connections/test_encrypted_creds_gcp.b64"),
        ],
    )
    assert "Created connection:" in cmd.output
    assert cmd.exit_code == 0
    connection_result = _parse_output(cmd.output)

    conn_with_creds = connections_api.get_connection_with_credentials(
        connection_result["id"]
    )
    assert (
        conn_with_creds.encrypted_credentials["gcp_kms"]["resource_name"]
        == GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME
    )
    assert (
        conn_with_creds.encrypted_credentials["gcp_kms"]["data"]
        == Path(get_fixture("connections/test_encrypted_creds_gcp.b64"))
        .read_text()
        .replace("\n", "")
        .strip()
    )

    # Create a connection with a connection config without plaintext credentials
    # and a pre-encrypted credentials file (raw bytes).
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection_without_creds.json"),
            "--gcp-kms-key-resource-name",
            GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME,
            "--gcp-kms-encrypted-credentials",
            get_fixture("connections/test_encrypted_creds_gcp.bin"),
        ],
    )
    assert "Created connection:" in cmd.output
    assert cmd.exit_code == 0
    connection_result = _parse_output(cmd.output)

    conn_with_creds = connections_api.get_connection_with_credentials(
        connection_result["id"]
    )
    assert (
        conn_with_creds.encrypted_credentials["gcp_kms"]["resource_name"]
        == GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME
    )
    assert conn_with_creds.encrypted_credentials["gcp_kms"]["data"] == base64.b64encode(
        Path(get_fixture("connections/test_encrypted_creds_gcp.bin")).read_bytes()
    ).decode("ascii")

    # Attempt to use a connection config with pre-encrypted credentials with
    # GCP KMS specific flags.
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection_pre_encrypted_creds_gcp.json"),
            "--gcp-kms-key-resource-name",
            GRETEL_CREDS_GCP_ENCRYPTION_KEY_RESOURCE_NAME,
        ],
    )
    assert "Encryption provider options must not be used" in cmd.output
    assert cmd.exit_code != 0

    # Create a connection using a config with pre-encrypted credentials.
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection_pre_encrypted_creds_gcp.json"),
        ],
    )
    assert "Created connection:" in cmd.output
    assert cmd.exit_code == 0
