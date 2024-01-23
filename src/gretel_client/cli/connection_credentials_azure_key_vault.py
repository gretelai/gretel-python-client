import functools
import re

from pathlib import Path
from typing import Optional

import click

import gretel_client._hybrid.azure as azure_hybrid

from gretel_client.cli.connection_credentials import (
    CredentialsEncryptionAdapter,
    CredentialsEncryptionFlagsBase,
)

# Format: https://myvaultname.vault.azure.net
VALID_VAULT_URL = re.compile(r"^https://[A-Za-z0-9_-]+\.vault\.azure\.net/?$")
# Format: https://myvaultname.vault.azure.net/keys/my-key-name
VALID_QUALIFIED_KEY_ID = re.compile(
    r"^https://[A-Za-z0-9_-]+\.vault\.azure\.net/keys/[A-Za-z0-9_-]+$"
)
VALID_KEY_ID = re.compile(r"^[A-Za-z0-9_-]+$")


class AzureKeyVaultEncryption(CredentialsEncryptionFlagsBase):
    @classmethod
    def _cli_decorate(cls, fn, param_name: str):
        @click.option(
            "--azure-key-vault-url",
            metavar="AZURE_KEY_VAULT_URL",
            help="Key Vault URL for Customer-managed credentials encryption. eg. https://myvault.vault.azure.net",
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
            help="Key Id to use during encryption. This is the same as the key name. eg. my-key-name",
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
            azure_encrypted_credentials: Optional[Path],
            **kwargs,
        ):
            kwargs[param_name] = cls._process_azure_key_vault_params(
                azure_key_vault_url,
                azure_key_id,
                azure_encryption_algorithm,
                azure_encrypted_credentials,
            )
            return fn(**kwargs)

        return proxy

    @staticmethod
    def _process_azure_key_vault_params(
        azure_key_vault_url: Optional[str],
        azure_key_id: Optional[str],
        azure_encryption_algorithm: Optional[str],
        azure_encrypted_credentials: Optional[Path],
    ) -> Optional[CredentialsEncryptionAdapter]:
        if all(
            item is None
            for item in [
                azure_key_vault_url,
                azure_key_id,
                azure_encryption_algorithm,
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
        if re.match(VALID_QUALIFIED_KEY_ID, azure_key_id):
            # Format: https://gretelkeyvault.vault.azure.net/keys/gretel-key
            azure_key_id = azure_key_id.split("/")[-1]
        if not re.match(VALID_KEY_ID, azure_key_id):
            raise click.UsageError("The key ID did not match the expected format.")

        if not azure_encryption_algorithm and azure_encrypted_credentials:
            raise click.UsageError(
                "--azure-encryption-algorithm must be set if --azure-encrypted-credentials are specified"
            )

        return CredentialsEncryptionAdapter(
            lambda: azure_hybrid.KeyVaultEncryption(
                azure_key_vault_url,
                azure_key_id,
                azure_encryption_algorithm,
            ),
            azure_encrypted_credentials,
        )
