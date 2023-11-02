import base64
import functools
import re

from pathlib import Path
from typing import Optional

import click

from gretel_client.cli.common import KVPairs
from gretel_client.cli.connection_credentials import CredentialsEncryption

VALID_VAULT_URL = re.compile(r"https://[A-Za-z0-9_-]+\.vault\.azure\.net/?")


class AzureKeyVaultEncryption(CredentialsEncryption):
    _vault_url: str
    _key_id: str
    _encryption_algorithm: str
    _encryption_context: dict

    def __init__(
        self,
        vault_url: str,
        key_id: str,
        encryption_algorithm: str,
        encryption_context: Optional[dict],
        encrypted_creds_file: Optional[Path],
    ):
        super().__init__(encrypted_creds_file)
        self._vault_url = vault_url
        self._key_id = key_id
        self._encryption_algorithm = encryption_algorithm or "RSA-OAEP"
        self._encryption_context = encryption_context or {}

    def _encrypt_payload(self, payload: bytes) -> bytes:
        # Encrypt credentials
        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.keys import KeyClient
            from azure.keyvault.keys.crypto import (
                CryptographyClient,
                EncryptionAlgorithm,
            )
        except ImportError as e:
            raise Exception(
                "You are trying to encrypt connection credentials with an Azure Keyvault key, "
                "but the Azure client libraries could not be found. If you want to use this "
                "feature, please re-install the Gretel CLI with the [azure] option.",
            ) from e

        credential = DefaultAzureCredential()

        # Example: https://myvault.vault.azure.net
        key_client = KeyClient(vault_url=self._vault_url, credential=credential)

        # Example: my-key-name
        key = key_client.get_key(self._key_id)
        crypto_client = CryptographyClient(key, credential=credential)

        # Example: RSA-OAEP
        encryption_algorithm = EncryptionAlgorithm(self._encryption_algorithm)

        result = crypto_client.encrypt(encryption_algorithm, payload)
        return result.ciphertext

    def _make_encrypted_creds_config(self, ciphertext: bytes) -> dict:
        vault_url_without_slash = (
            self._vault_url[:-1] if self._vault_url.endswith("/") else self._vault_url
        )
        key_id = f"{vault_url_without_slash}/keys/{self._key_id}"
        return {
            "azure_key_vault": {
                "key_id": key_id,
                "encryption_context": self._encryption_context,
                "encryption_algorithm": self._encryption_algorithm,
                "data": base64.b64encode(ciphertext).decode("ascii"),
            }
        }

    @classmethod
    def cli_decorate(cls, fn, param_name: str):
        @click.option(
            "--azure-key-vault-url",
            metavar="AZURE_KEY_VAULT_URL",
            help="Key Vault URL for Customer-managed credentials encryption",
            required=False,
        )
        @click.option(
            "--azure-encryption-algorithm",
            metavar="AZURE_ENCRYPTION_ALGORITHM",
            help="Algorithm used to encrypt the credentials",
            required=False,
        )
        @click.option(
            "--azure-key-id",
            metavar="AZURE_KEY_ID",
            help="Key Id from key vault to use during encryption",
            required=False,
        )
        @click.option(
            "--azure-encryption-context",
            metavar="KEY1=VALUE1[,...,KEYn=VALUEn]",
            help="Comma-separated key/value pairs for Azure Key Vault encryption context",
            type=KVPairs,
            required=False,
        )
        @click.option(
            "--azure-encrypted-credentials",
            metavar="FILE",
            help="Path to the file containing the credentials encrypted using Azure Key Vault",
            type=click.Path(
                exists=True,
                dir_okay=False,
                path_type=Path,
            ),
            required=False,
        )
        @functools.wraps(fn)
        def proxy(
            azure_key_vault_url: Optional[str],
            azure_key_id: Optional[str],
            azure_encryption_algorithm: Optional[str],
            azure_encryption_context: Optional[dict],
            azure_encrypted_credentials: Optional[Path],
            **kwargs,
        ):
            kwargs[param_name] = cls._process_azure_key_vault_params(
                azure_key_vault_url,
                azure_key_id,
                azure_encryption_algorithm,
                azure_encryption_context,
                azure_encrypted_credentials,
            )
            return fn(**kwargs)

        return proxy

    @classmethod
    def _process_azure_key_vault_params(
        cls,
        azure_key_vault_url: Optional[str],
        azure_key_id: Optional[str],
        azure_encryption_algorithm: Optional[str],
        azure_encryption_context: Optional[dict],
        azure_encrypted_credentials: Optional[Path],
    ) -> Optional["AzureKeyVaultEncryption"]:
        if all(
            item is None
            for item in [
                azure_key_vault_url,
                azure_key_id,
                azure_encryption_algorithm,
                azure_encryption_context,
                azure_encrypted_credentials,
            ]
        ):
            return None

        if azure_key_id is None:
            raise click.UsageError(
                "The --azure-key-id option must always be specified in conjunction with other Azure Key Vault options",
            )
        if not re.match(VALID_VAULT_URL, azure_key_vault_url):
            raise click.UsageError(
                "The keyvault URL must match the format 'https://{vault-name}.vault.azure.net'"
            )

        if not azure_encryption_algorithm and azure_encrypted_credentials:
            raise click.UsageError(
                "--azure-encryption-algorithm must be set if --azure-encrypted-credentials are specified"
            )

        return AzureKeyVaultEncryption(
            azure_key_vault_url,
            azure_key_id,
            azure_encryption_algorithm,
            azure_encryption_context,
            azure_encrypted_credentials,
        )
