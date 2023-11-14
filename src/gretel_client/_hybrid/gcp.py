import re

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from google.cloud import kms

from gretel_client._hybrid.creds_encryption import CredentialsEncryption

# Valid case 1: "projects/<PROJECT_ID>/locations/<LOCATION>/keyRings/<KEY_RING_ID>/cryptoKeys/<KEY_ID>"
# Valid case 2: "//cloudkms.googleapis.com/projects/<PROJECT_ID>/locations/<LOCATION>/keyRings/<KEY_RING_ID>/cryptoKeys/<KEY_ID>"
# See: https://regex101.com/r/HNe6BX/3
_REGEX_KMS_PATTERN = re.compile(
    "^(?://cloudkms\.googleapis\.com/)?projects/(?P<PROJECT_ID>[^/]+)/locations/(?P<LOCATION>[^/]+)/keyRings/(?P<KEY_RING_ID>[^/]+)/cryptoKeys/(?P<KEY_ID>[^/\s]+)$"
)


class KMSEncryption(CredentialsEncryption):

    _kms_client: Any

    def __init__(
        self,
        key_resource_name: str,
    ):
        if not _REGEX_KMS_PATTERN.match(key_resource_name):
            raise ValueError(
                f"Malformed GCP KMS key resource name: {key_resource_name}"
            )

        self._key_resource_name = key_resource_name
        try:
            from google.cloud import kms
        except ImportError as e:
            raise Exception(
                "You are trying to encrypt connection credentials with a GCP KMS key, "
                "but the GCP client libraries could not be found. If you want to use this "
                "feature, please re-install the Gretel CLI with the [gcp] option.",
            ) from e

        self._kms_client = kms.KeyManagementServiceClient()

    def _encrypt_payload(self, payload: bytes) -> bytes:
        encrypt_response = self._kms_client.encrypt(
            request={
                "name": self._key_resource_name,
                "plaintext": payload,
            }
        )
        return encrypt_response.ciphertext

    def make_encrypted_creds_config(self, ciphertext_b64: str) -> dict:
        return {
            "gcp_kms": {
                "resource_name": self._key_resource_name,
                "data": ciphertext_b64,
            }
        }
