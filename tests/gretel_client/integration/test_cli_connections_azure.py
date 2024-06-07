import json
import os

from pathlib import Path
from typing import Callable
from unittest.mock import Mock, patch

import pytest

from click.testing import CliRunner

from gretel_client._hybrid.azure import KeyVaultEncryption
from gretel_client.cli.cli import cli
from gretel_client.projects.projects import Project
from gretel_client.rest_v1.api.connections_api import ConnectionsApi

GRETEL_CREDS_ENCRYPTION_AZURE_KEY_ID = os.getenv("GRETEL_CREDS_ENCRYPTION_AZURE_KEY_ID")

GRETEL_CREDS_ENCRYPTION_AZURE_KEY_VAULT_URL = os.getenv(
    "GRETEL_CREDS_ENCRYPTION_AZURE_KEY_VAULT_URL"
)


def _parse_output(output: str) -> dict:
    return json.loads(output.rsplit("Created connection:\n")[1])


def test_azure_connection_validations(get_fixture: Callable, project: Project):
    runner = CliRunner()
    key_vault_url = "https://fake.vault.azure.net/"
    key_id = "fake"

    # Invalid Keyvault URL
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection.json"),
            "--azure-key-id",
            "fake",
            "--azure-key-vault-url",
            "https://fake-vault.com",
        ],
    )
    assert "The keyvault URL must match the format" in cmd.output
    assert cmd.exit_code != 0

    # Only pass a key vault URL, no key id
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
            "https://fake.vault.azure.net/",
        ],
    )
    assert "--azure-key-id" in cmd.output
    assert cmd.exit_code != 0

    # Only pass an encrypted credentials file, no key id
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
            "https://fake.vault.azure.net/",
            "--azure-encrypted-credentials",
            get_fixture("connections/test_encrypted_creds.b64"),
        ],
    )
    assert "--azure-key-id" in cmd.output
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
            "--azure-key-vault-url",
            "https://fake.vault.azure.net/",
            "--azure-key-id",
            "fake",
        ],
    )
    assert "An encrypted credentials file must be specified" in cmd.output
    assert cmd.exit_code != 0

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
            "--azure-key-vault-url",
            key_vault_url,
            "--azure-key-id",
            key_id,
            "--azure-encryption-algorithm",
            "RSA-OAEP",
            "--azure-encrypted-credentials",
            get_fixture("connections/test_encrypted_creds.b64"),
        ],
    )
    assert "An encrypted credentials file must not be specified" in cmd.output
    assert cmd.exit_code != 0

    # Attempt to use a connection config with pre-encrypted credentials with a
    # Azure Key Vault specific flags.
    cmd = runner.invoke(
        cli,
        [
            "connections",
            "create",
            "--project",
            project.project_guid,  # type: ignore
            "--from-file",
            get_fixture("connections/test_connection_pre_encrypted_creds.json"),
            "--azure-key-vault-url",
            key_vault_url,
            "--azure-key-id",
            key_id,
        ],
    )
    assert "Encryption provider options must not be used" in cmd.output
    assert cmd.exit_code != 0

    # Test if an encryption algorithm is not passed when a pre-encrypted file is passed
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
            key_vault_url,
            "--azure-key-id",
            key_id,
            "--azure-encrypted-credentials",
            get_fixture("connections/test_encrypted_creds.b64"),
        ],
    )
    assert "--azure-encryption-algorithm must be set" in cmd.output
    assert cmd.exit_code != 0


@pytest.mark.skipif(
    not GRETEL_CREDS_ENCRYPTION_AZURE_KEY_ID,
    reason="Skipping hybrid tests based on missing GRETEL_CREDS_ENCRYPTION_AZURE_KEY_ID environment variable",
)
def test_azure_connection_crud_from_cli(
    get_fixture: Callable,
    project: Project,
    connections_api: ConnectionsApi,
):
    key_id = GRETEL_CREDS_ENCRYPTION_AZURE_KEY_ID
    key_vault_url = GRETEL_CREDS_ENCRYPTION_AZURE_KEY_VAULT_URL
    runner = CliRunner()

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
            "--azure-key-vault-url",
            key_vault_url,
            "--azure-key-id",
            key_id,
        ],
    )
    print(cmd.output)
    assert "Created connection:" in cmd.output
    assert cmd.exit_code == 0
    connection_result = _parse_output(cmd.output)

    conn_with_creds = connections_api.get_connection_with_credentials(
        connection_result["id"]
    )
    expected_key_id = f"{key_vault_url}/keys/{key_id}".replace("//keys", "/keys")
    assert (
        conn_with_creds.encrypted_credentials["azure_key_vault"]["key_id"]
        == expected_key_id
    )

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
            "--azure-key-vault-url",
            key_vault_url,
            "--azure-key-id",
            key_id,
            "--azure-encryption-algorithm",
            "RSA-OAEP",
            "--azure-encrypted-credentials",
            get_fixture("connections/test_encrypted_creds.b64"),
        ],
    )
    assert "Created connection:" in cmd.output
    assert cmd.exit_code == 0
    connection_result = _parse_output(cmd.output)

    conn_with_creds = connections_api.get_connection_with_credentials(
        connection_result["id"]
    )
    assert (
        conn_with_creds.encrypted_credentials["azure_key_vault"]["key_id"]
        == expected_key_id
    )
    assert (
        conn_with_creds.encrypted_credentials["azure_key_vault"]["data"]
        == Path(get_fixture("connections/test_encrypted_creds.b64"))
        .read_text("utf-8")
        .replace("\n", "")
        .strip()
    )


@patch("azure.keyvault.keys.KeyClient")
@patch("azure.keyvault.keys.crypto.CryptographyClient")
def test_key_vault_encryption_validation(
    cryptography_client_patch: Mock, key_client_patch: Mock
):
    with pytest.raises(ValueError, match="Malformed Azure Key Vault URL"):
        KeyVaultEncryption("https://dev.vault.dfs", "abc")
    with pytest.raises(ValueError, match="Malformed Azure Key Vault Key ID"):
        KeyVaultEncryption(
            "https://dev.vault.usgovcloudapi.net",
            "https://dev.vault.usgovcloudapif.net/keys/abc",
        )

    KeyVaultEncryption("https://dev.vault.usgovcloudapi.net", "abc")
    key_client_patch.assert_called_once()
    cryptography_client_patch.assert_called_once()

    key_client_patch.reset_mock()
    cryptography_client_patch.reset_mock()
    KeyVaultEncryption("https://dev.vault.azure.net", "abc")
    key_client_patch.assert_called_once()
    cryptography_client_patch.assert_called_once()
