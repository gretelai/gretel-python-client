import base64
import functools
import json
import re

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

import click

from gretel_client.cli.common import KVPairs

_STD_B64_RE = re.compile(r"^[\sa-zA-Z0-9/+]+(?:=\s*){,2}$")
_URL_B64_RE = re.compile(r"^[\sa-zA-Z0-9_-]+(?:=\s*){,2}$")


class CredentialsEncryption(ABC):
    @abstractmethod
    def apply(self, credentials: Optional[dict]) -> dict:
        """
        Applies encryption to the credentials of a connection.

        Args:
            creds: the plaintext credentials to be encrypted. These may be unset
                to support passing in the encrypted credentials ciphertext through
                other channels.

        Returns:
            the ``encrypted_credentials`` config section to be used in the
            connection.
        """
        ...


class AWSKMSEncryption(CredentialsEncryption):

    _key_arn: str
    _key_region: str
    _encryption_context: dict
    _encrypted_creds_file: Optional[Path]

    def __init__(
        self,
        key_arn: str,
        key_region: str,
        encryption_context: Optional[dict],
        encrypted_creds_file: Optional[Path],
    ):
        self._key_arn = key_arn
        self._key_region = key_region
        self._encryption_context = encryption_context or {}
        self._encrypted_creds_file = encrypted_creds_file

    def _get_ciphertext(self, creds: Optional[dict] = None) -> bytes:
        if creds is None:
            if self._encrypted_creds_file is None:
                raise ValueError(
                    "An encrypted credentials file must be specified if "
                    "the connection config does not contain plaintext "
                    "credentials"
                )

            # We want to support both base64 and raw bytes as the format
            # for the encrypted credentials file. If the contents of the file
            # are valid b64, we assume it is b64.
            # The chance that the encrypted raw ciphertext happens to be valid
            # b64 is extremely small; in the off-chance that this ever poses an
            # issue, we can provide a simple workaround: simply b64 encode the
            # file, as we won't attempt to b64 decode twice.
            data = self._encrypted_creds_file.read_bytes()
            try:
                str_data = data.decode("ascii")
                if _STD_B64_RE.match(str_data):
                    data = base64.standard_b64decode(data)
                elif _URL_B64_RE.match(str_data):
                    data = base64.urlsafe_b64decode(data)
            except:
                # Simply ignore any error and use the raw data
                pass

            return data

        if self._encrypted_creds_file is not None:
            raise ValueError(
                "An encrypted credentials file must not be specified if "
                "the connection config contains plaintext credentials"
            )

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
        creds_json = json.dumps(creds)
        encrypt_response = kms_client.encrypt(
            KeyId=self._key_arn,
            Plaintext=creds_json.encode("utf-8"),
            EncryptionContext=self._encryption_context,
        )
        return encrypt_response["CiphertextBlob"]

    def apply(self, credentials: Optional[dict]) -> dict:
        ciphertext = self._get_ciphertext(credentials)

        return {
            "aws_kms": {
                "key_arn": self._key_arn,
                "encryption_context": self._encryption_context,
                "data": base64.b64encode(ciphertext).decode("ascii"),
            }
        }


def _aws_kms_decorate(fn, param_name: str):
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
        kwargs[param_name] = _process_aws_kms_params(
            aws_kms_key_arn,
            aws_kms_encryption_context,
            aws_kms_encrypted_credentials,
        )
        return fn(**kwargs)

    return proxy


def _process_aws_kms_params(
    aws_kms_key_arn: Optional[str],
    aws_kms_encryption_context: Optional[dict],
    aws_kms_encrypted_credentials: Optional[Path],
) -> Optional[AWSKMSEncryption]:
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


def aws_kms_flags(param_name: str):
    """
    Returns a decorator to add AWS KMS-related flags to a command function.

    Args:
        param_name: the name of the parameter of the function to be decorated
            receiving the ``Optional[AWSKMSEncryption]`` value.

    Returns:
        a decorator that adds AWS KMS-related flags to a command function,
        exposing them via a single ``Optional[AWSKMSEncryption]``-typed
        value.
    """

    def wrapper(fn):
        return _aws_kms_decorate(fn, param_name)

    return wrapper
