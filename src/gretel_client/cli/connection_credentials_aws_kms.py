import base64
import functools

from pathlib import Path
from typing import Optional

import click

import gretel_client._hybrid.aws as aws_hybrid

from gretel_client.cli.common import KVPairs
from gretel_client.cli.connection_credentials import (
    CredentialsEncryptionAdapter,
    CredentialsEncryptionFlagsBase,
)


class AWSKMSEncryption(CredentialsEncryptionFlagsBase):
    @classmethod
    def _cli_decorate(cls, fn, param_name: str):
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

    @staticmethod
    def _process_cli_params(
        aws_kms_key_arn: Optional[str],
        aws_kms_encryption_context: Optional[dict],
        aws_kms_encrypted_credentials: Optional[Path],
    ) -> Optional[CredentialsEncryptionAdapter]:
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

        return CredentialsEncryptionAdapter(
            aws_hybrid.KMSEncryption(aws_kms_key_arn, aws_kms_encryption_context),
            aws_kms_encrypted_credentials,
        )
