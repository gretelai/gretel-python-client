import base64
import functools
import re

from pathlib import Path
from typing import Optional

import click

from gretel_client.cli.connection_credentials import CredentialsEncryption

# Valid case 1: "projects/<PROJECT_ID>/locations/<LOCATION>/keyRings/<KEY_RING_ID>/cryptoKeys/<KEY_ID>"
# Valid case 2: "//cloudkms.googleapis.com/projects/<PROJECT_ID>/locations/<LOCATION>/keyRings/<KEY_RING_ID>/cryptoKeys/<KEY_ID>"
# See: https://regex101.com/r/HNe6BX/3
regex_kms_pattern = re.compile(
    "^(?://cloudkms\.googleapis\.com/)?projects/(?P<PROJECT_ID>[^/]+)/locations/(?P<LOCATION>[^/]+)/keyRings/(?P<KEY_RING_ID>[^/]+)/cryptoKeys/(?P<KEY_ID>[^/\s]+)$"
)


class GCPKMSEncryption(CredentialsEncryption):
    def __init__(
        self,
        key_resource_name: str,
        encrypted_creds_file: Optional[Path],
    ):
        self._key_resource_name = key_resource_name
        self._encrypted_creds_file = encrypted_creds_file

    def _encrypt_payload(self, payload: bytes) -> bytes:
        try:
            from google.cloud import kms
        except ImportError as e:
            raise Exception(
                "You are trying to encrypt connection credentials with a GCP KMS key, "
                "but the GCP client libraries could not be found. If you want to use this "
                "feature, please re-install the Gretel CLI with the [gcp] option.",
            ) from e

        kms_client = kms.KeyManagementServiceClient()
        encrypt_response = kms_client.encrypt(
            request={
                "name": self._key_resource_name,
                "plaintext": payload,
            }
        )
        return encrypt_response.ciphertext

    def _make_encrypted_creds_config(self, ciphertext: bytes) -> dict:
        return {
            "gcp_kms": {
                "resource_name": self._key_resource_name,
                "data": base64.b64encode(ciphertext).decode("ascii"),
            }
        }

    @classmethod
    def cli_decorate(cls, fn, param_name: str):
        @click.option(
            "--gcp-kms-key-resource-name",
            metavar="KEY-RN",
            help="Resource name of the GCP KMS key used for Customer-managed credentials encryption",
            required=False,
        )
        @click.option(
            "--gcp-kms-encrypted-credentials",
            metavar="FILE",
            help="Path to the file containing the credentials encrypted using GCP KMS",
            type=click.Path(
                exists=True,
                dir_okay=False,
                path_type=Path,
            ),
            required=False,
        )
        @functools.wraps(fn)
        def proxy(
            gcp_kms_key_resource_name: Optional[str],
            gcp_kms_encrypted_credentials: Optional[Path],
            **kwargs,
        ):
            kwargs[param_name] = cls._process_cli_params(
                gcp_kms_key_resource_name,
                gcp_kms_encrypted_credentials,
            )
            return fn(**kwargs)

        return proxy

    @classmethod
    def _process_cli_params(
        cls,
        gcp_kms_key_resource_name: Optional[str],
        gcp_kms_encrypted_credentials: Optional[Path],
    ) -> Optional["GCPKMSEncryption"]:
        if gcp_kms_key_resource_name is None and gcp_kms_encrypted_credentials is None:
            return None

        if gcp_kms_key_resource_name is None:
            raise click.UsageError(
                "The --gcp-kms-key-resource-name option must always be specified in conjunction with other GCP KMS options",
            )

        if not regex_kms_pattern.match(gcp_kms_key_resource_name):
            raise click.BadOptionUsage(
                "--gcp-kms-key-resource-name",
                f"Key resource name {gcp_kms_key_resource_name} is invalid",
            )

        return GCPKMSEncryption(
            gcp_kms_key_resource_name,
            gcp_kms_encrypted_credentials,
        )
