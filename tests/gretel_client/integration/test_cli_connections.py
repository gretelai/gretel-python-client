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


def _parse_output(output: str) -> dict:
    return json.loads(output.rsplit("Created connection:\n")[1])


def test_connection_crud_from_cli(get_fixture: Callable, project: Project):
    runner = CliRunner()

    # Create a connection
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection.json"),
        ],
    )
    assert "Created connection:" in cmd.output
    assert project.project_guid in cmd.output  # type: ignore
    assert cmd.exit_code == 0
    connection_result = _parse_output(cmd.output)
    print(cmd.output)

    # Get connections
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "list",
        ],
    )
    assert "Connections:" in cmd.output
    assert connection_result["id"] in cmd.output
    assert cmd.exit_code == 0

    # Get a connection by id
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "get",
            "--id",
            connection_result["id"],
        ],
    )
    assert "Connection:" in cmd.output
    assert connection_result["id"] in cmd.output
    assert cmd.exit_code == 0

    # Update a connection by id
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "update",
            "--id",
            connection_result["id"],
            "--from-file",
            get_fixture("connections/test_connection_edited.json"),
        ],
    )
    assert "Updated connection:" in cmd.output
    assert "integration_test_edited" in cmd.output
    assert cmd.exit_code == 0

    # Delete a connection by id
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "delete",
            "--id",
            connection_result["id"],
        ],
    )
    assert "Deleted connection " + connection_result["id"] in cmd.output
    assert cmd.exit_code == 0


@pytest.mark.skipif(
    not os.getenv("GRETEL_CREDS_ENCRYPTION_KEY_ARN"),
    reason="Skipping hybrid tests based on missing GRETEL_CREDS_ENCRYPTION_KEY_ARN environment variable",
)
def test_hybrid_connection_crud_from_cli(
    get_fixture: Callable,
    project: Project,
    connections_api: ConnectionsApi,
):
    key_arn = os.getenv("GRETEL_CREDS_ENCRYPTION_KEY_ARN")

    runner = CliRunner()

    # Only pass an encryption context, no key ARN
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection.json"),
            "--aws-kms-encryption-context",
            "foo=bar,baz=qux",
        ],
    )
    assert "--aws-kms-key-arn" in cmd.output
    assert cmd.exit_code != 0

    # Only pass an encrypted credentials file, no key ARN
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection.json"),
            "--aws-kms-encrypted-credentials",
            get_fixture("connections/test_encrypted_creds.b64"),
        ],
    )
    assert "--aws-kms-key-arn" in cmd.output
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
            "--aws-kms-key-arn",
            key_arn,
            "--aws-kms-encryption-context",
            "foo=bar,baz=qux",
        ],
    )
    assert "Created connection:" in cmd.output
    assert cmd.exit_code == 0
    connection_result = _parse_output(cmd.output)

    conn_with_creds = connections_api.get_connection_with_credentials(
        connection_result["id"]
    )
    assert conn_with_creds.encrypted_credentials["aws_kms"]["key_arn"] == key_arn
    assert conn_with_creds.encrypted_credentials["aws_kms"]["encryption_context"] == {
        "foo": "bar",
        "baz": "qux",
    }

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
            "--aws-kms-key-arn",
            key_arn,
            "--aws-kms-encryption-context",
            "foo=bar,baz=qux",
            "--aws-kms-encrypted-credentials",
            get_fixture("connections/test_encrypted_creds.b64"),
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
            "--aws-kms-key-arn",
            key_arn,
            "--aws-kms-encryption-context",
            "foo=bar,baz=qux",
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
            "--aws-kms-key-arn",
            key_arn,
            "--aws-kms-encryption-context",
            "foo=bar,baz=qux",
            "--aws-kms-encrypted-credentials",
            get_fixture("connections/test_encrypted_creds.b64"),
        ],
    )
    assert "Created connection:" in cmd.output
    assert cmd.exit_code == 0
    connection_result = _parse_output(cmd.output)

    conn_with_creds = connections_api.get_connection_with_credentials(
        connection_result["id"]
    )
    assert conn_with_creds.encrypted_credentials["aws_kms"]["key_arn"] == key_arn
    assert conn_with_creds.encrypted_credentials["aws_kms"]["encryption_context"] == {
        "foo": "bar",
        "baz": "qux",
    }
    assert (
        conn_with_creds.encrypted_credentials["aws_kms"]["data"]
        == Path(get_fixture("connections/test_encrypted_creds.b64"))
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
            "--aws-kms-key-arn",
            key_arn,
            "--aws-kms-encryption-context",
            "foo=bar,baz=qux",
            "--aws-kms-encrypted-credentials",
            get_fixture("connections/test_encrypted_creds.bin"),
        ],
    )
    assert "Created connection:" in cmd.output
    assert cmd.exit_code == 0
    connection_result = _parse_output(cmd.output)

    conn_with_creds = connections_api.get_connection_with_credentials(
        connection_result["id"]
    )
    assert conn_with_creds.encrypted_credentials["aws_kms"]["key_arn"] == key_arn
    assert conn_with_creds.encrypted_credentials["aws_kms"]["encryption_context"] == {
        "foo": "bar",
        "baz": "qux",
    }
    assert conn_with_creds.encrypted_credentials["aws_kms"]["data"] == base64.b64encode(
        Path(get_fixture("connections/test_encrypted_creds.bin")).read_bytes()
    ).decode("ascii")

    # Attempt to use a connection config with pre-encrypted credentials with a
    # AWS KMS specific flags.
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection_pre_encrypted_creds.json"),
            "--aws-kms-key-arn",
            key_arn,
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
            get_fixture("connections/test_connection_pre_encrypted_creds.json"),
        ],
    )
    assert "Created connection:" in cmd.output
    assert cmd.exit_code == 0

    # Bad azure key vault string throws error.
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection.json"),
            "--azure-key-vault-url",
            "bad-vault-string",
            "--azure-key-id",
            "gretel-key",
        ],
    )
    assert "The keyvault URL must match the format" in cmd.output
    assert cmd.exit_code != 0

    # Bad azure key id string throws error.
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection.json"),
            "--azure-key-vault-url",
            "https://gretelkeyvault.vault.azure.net/",
            "--azure-key-id",
            "malformed-key-id/keys/gretel-key",
        ],
    )
    assert "The key ID did not match the expected format" in cmd.output
    assert cmd.exit_code != 0


def test_read_yaml(get_fixture: Callable):
    json_conn = _read_connection_file(get_fixture("connections/test_connection.json"))
    yaml_conn = _read_connection_file(get_fixture("connections/test_connection.yaml"))
    assert json_conn == yaml_conn


def test_file_not_found():
    with pytest.raises(OSError):
        _read_connection_file("bad/path")
