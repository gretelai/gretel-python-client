import base64
import re

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.keys import KeyClient
    from azure.keyvault.keys.crypto import (
        CryptographyClient,
        EncryptionAlgorithm,
    )
    from Crypto.Cipher import AES
    from Crypto.Random import get_random_bytes
    from Crypto.Util.Padding import pad

from gretel_client._hybrid.creds_encryption import CredentialsEncryption

# Format: https://myvaultname.vault.azure.net/
# or      https://myvaultname.vault.usgovcloudapi.net/
_VALID_VAULT_URL = re.compile(
    r"https://[A-Za-z0-9_-]+(\.vault\.azure\.net|\.vault\.usgovcloudapi\.net)/?"
)
# Format: https://myvaultname.vault.azure.net/keys/my-key-name
# or:     https://myvaultname.vault.usgovcloudapi.net/keys/my-key-name
_VALID_QUALIFIED_KEY_ID = re.compile(
    r"^https://[A-Za-z0-9_-]+(\.vault\.azure\.net|\.vault\.usgovcloudapi\.net)keys/[A-Za-z0-9_-]+$"
)
_VALID_KEY_ID = re.compile(r"^[A-Za-z0-9_-]+$")


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
        if _VALID_QUALIFIED_KEY_ID.match(key_id):
            key_id = key_id.split("/")[-1]
        if not _VALID_KEY_ID.match(key_id):
            raise ValueError(f"Malformed Azure Key Vault Key ID {key_id}")

        if not encryption_algorithm:
            encryption_algorithm = "RSA-OAEP"

        vault_url = vault_url.rstrip("/")
        self._qualified_key_id = f"{vault_url}/keys/{key_id}"

        self.client_key = None

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
        try:
            from Crypto.Cipher import AES
            from Crypto.Random import get_random_bytes
            from Crypto.Util.Padding import pad
        except ImportError as e:
            raise Exception(
                "You are trying to encrypt connection credentials with an Azure Keyvault key, "
                "but pycryptodome is required for creating the data key. If you want to use this "
                "feature, please re-install the Gretel CLI with the [azure] option.",
            ) from e

        self.client_key = get_random_bytes(32)
        cipher = AES.new(self.client_key, AES.MODE_CBC)

        cipher_text = cipher.encrypt(pad(payload, 16, style="pkcs7"))
        # Store the IV in the first 16 bytes
        return cipher.iv + cipher_text

    def _kv_encrypt_key(self, payload: bytes) -> bytes:
        result = self._crypto_client.encrypt(self._encryption_algorithm, payload)
        return base64.b64encode(result.ciphertext).decode("ascii")

    def make_encrypted_creds_config(self, ciphertext_b64: str) -> dict:
        creds_config = {
            "azure_key_vault": {
                "key_id": self._qualified_key_id,
                "encryption_algorithm": self._encryption_algorithm.value,
                "data": ciphertext_b64,
            }
        }
        # This is the case for all not pre-encrypted credentials
        if self.client_key is not None:
            encrypted_key = self._kv_encrypt_key(self.client_key)
            del self.client_key
            creds_config["azure_key_vault"]["encrypted_client_key"] = encrypted_key
        return creds_config
