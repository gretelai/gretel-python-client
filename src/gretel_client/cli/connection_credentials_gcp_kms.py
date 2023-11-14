import base64
import functools
import re

from pathlib import Path
from typing import Optional

import click

import gretel_client._hybrid.gcp as gcp_hybrid

from gretel_client.cli.connection_credentials import (
    CredentialsEncryptionAdapter,
    CredentialsEncryptionFlagsBase,
)

# Valid case 1: "projects/<PROJECT_ID>/locations/<LOCATION>/keyRings/<KEY_RING_ID>/cryptoKeys/<KEY_ID>"
# Valid case 2: "//cloudkms.googleapis.com/projects/<PROJECT_ID>/locations/<LOCATION>/keyRings/<KEY_RING_ID>/cryptoKeys/<KEY_ID>"
# See: https://regex101.com/r/HNe6BX/3
regex_kms_pattern = re.compile(
    "^(?://cloudkms\.googleapis\.com/)?projects/(?P<PROJECT_ID>[^/]+)/locations/(?P<LOCATION>[^/]+)/keyRings/(?P<KEY_RING_ID>[^/]+)/cryptoKeys/(?P<KEY_ID>[^/\s]+)$"
)


class GCPKMSEncryption(CredentialsEncryptionFlagsBase):
    @classmethod
    def _cli_decorate(cls, fn, param_name: str):
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

    @staticmethod
    def _process_cli_params(
        gcp_kms_key_resource_name: Optional[str],
        gcp_kms_encrypted_credentials: Optional[Path],
    ) -> Optional[CredentialsEncryptionAdapter]:
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

        return CredentialsEncryptionAdapter(
            gcp_hybrid.KMSEncryption(gcp_kms_key_resource_name),
            gcp_kms_encrypted_credentials,
        )
