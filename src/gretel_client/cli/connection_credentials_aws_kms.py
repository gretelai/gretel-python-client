import base64
import functools

from pathlib import Path
from typing import Optional

import click

from gretel_client.cli.common import KVPairs
from gretel_client.cli.connection_credentials import CredentialsEncryption


class AWSKMSEncryption(CredentialsEncryption):

    _key_arn: str
    _key_region: str
    _encryption_context: dict

    def __init__(
        self,
        key_arn: str,
        key_region: str,
        encryption_context: Optional[dict],
        encrypted_creds_file: Optional[Path],
    ):
        super().__init__(encrypted_creds_file)
        self._key_arn = key_arn
        self._key_region = key_region
        self._encryption_context = encryption_context or {}

    def _encrypt_payload(self, payload: bytes) -> bytes:
        # Encrypt credentials
        try:
            import boto3
        except ImportError as e:
            raise Exception(
                "You are trying to encrypt connection credentials with an AWS KMS key, "
                "but the AWS client libraries could not be found. If you want to use this "
                "feature, please re-install the Gretel CLI with the [aws] option.",
            ) from e

        kms_client = boto3.client("kms", region_name=self._key_region)
        encrypt_response = kms_client.encrypt(
            KeyId=self._key_arn,
            Plaintext=payload,
            EncryptionContext=self._encryption_context,
        )
        return encrypt_response["CiphertextBlob"]

    def _make_encrypted_creds_config(self, ciphertext: bytes) -> dict:
        return {
            "aws_kms": {
                "key_arn": self._key_arn,
                "encryption_context": self._encryption_context,
                "data": base64.b64encode(ciphertext).decode("ascii"),
            }
        }

    @classmethod
    def cli_decorate(cls, fn, param_name: str):
        @click.option(
            "--aws-kms-key-arn",
            metavar="KEY-ARN",
            help="ARN of the AWS KMS key used for Customer-managed credentials encryption",
            required=False,
        )
        @click.option(
            "--aws-kms-encryption-context",
            metavar="KEY1=VALUE1[,...,KEYn=VALUEn]",
            help="Comma-separated key/value pairs for AWS KMS encryption context",
            type=KVPairs,
            required=False,
        )
        @click.option(
            "--aws-kms-encrypted-credentials",
            metavar="FILE",
            help="Path to the file containing the credentials encrypted using AWS KMS",
            type=click.Path(
                exists=True,
                dir_okay=False,
                path_type=Path,
            ),
            required=False,
        )
        @functools.wraps(fn)
        def proxy(
            aws_kms_key_arn: Optional[str],
            aws_kms_encryption_context: Optional[dict],
            aws_kms_encrypted_credentials: Optional[Path],
            **kwargs,
        ):
            kwargs[param_name] = cls._process_cli_params(
                aws_kms_key_arn,
                aws_kms_encryption_context,
                aws_kms_encrypted_credentials,
            )
            return fn(**kwargs)

        return proxy

    @classmethod
    def _process_cli_params(
        cls,
        aws_kms_key_arn: Optional[str],
        aws_kms_encryption_context: Optional[dict],
        aws_kms_encrypted_credentials: Optional[Path],
    ) -> Optional["AWSKMSEncryption"]:
        if (
            aws_kms_key_arn is None
            and aws_kms_encryption_context is None
            and aws_kms_encrypted_credentials is None
        ):
            return None

        if aws_kms_key_arn is None:
            raise click.UsageError(
                "The --aws-kms-key-arn option must always be specified in conjunction with other AWS KMS options",
            )

        parsed_arn = aws_kms_key_arn.split(":", 5)
        if parsed_arn[:3] != ["arn", "aws", "kms"] or len(parsed_arn) < 6:
            raise click.BadOptionUsage(
                "--aws-kms-key-arn",
                f"Key ARN {aws_kms_key_arn} is invalid",
            )

        key_region = parsed_arn[3]

        return AWSKMSEncryption(
            aws_kms_key_arn,
            key_region,
            aws_kms_encryption_context,
            aws_kms_encrypted_credentials,
        )
