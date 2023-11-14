import re

from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.keys import KeyClient
    from azure.keyvault.keys.crypto import (
        CryptographyClient,
        EncryptionAlgorithm,
    )

from gretel_client._hybrid.creds_encryption import CredentialsEncryption

_VALID_VAULT_URL = re.compile(r"https://[A-Za-z0-9_-]+\.vault\.azure\.net/?")


class KeyVaultEncryption(CredentialsEncryption):

    _qualified_key_id: str
    _crypto_client: "CryptographyClient"
    _encryption_algorithm: "EncryptionAlgorithm"

    def __init__(
        self,
        vault_url: str,
        key_id: str,
        encryption_algorithm: Optional[str] = None,
    ):
        if not _VALID_VAULT_URL.match(vault_url):
            raise ValueError(f"Malformed Azure Key Vault URL {vault_url}")

        if not encryption_algorithm:
            encryption_algorithm = "RSA-OAEP"

        vault_url = vault_url.rstrip("/")
        self._qualified_key_id = f"{vault_url}/keys/{key_id}"

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
        key_client = KeyClient(vault_url=vault_url, credential=credential)

        # Example: my-key-name
        key = key_client.get_key(key_id)
        self._crypto_client = CryptographyClient(key, credential=credential)

        # Example: RSA-OAEP
        self._encryption_algorithm = EncryptionAlgorithm(encryption_algorithm)

    def _encrypt_payload(self, payload: bytes) -> bytes:
        result = self._crypto_client.encrypt(self._encryption_algorithm, payload)
        return result.ciphertext

    def make_encrypted_creds_config(self, ciphertext_b64: str) -> dict:
        return {
            "azure_key_vault": {
                "key_id": self._qualified_key_id,
                "encryption_algorithm": self._encryption_algorithm.value,
                "data": ciphertext_b64,
            }
        }
