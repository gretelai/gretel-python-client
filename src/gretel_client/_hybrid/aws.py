from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import boto3

from gretel_client._hybrid.creds_encryption import CredentialsEncryption


class KMSEncryption(CredentialsEncryption):
    """
    Credentials encryption mechanism using AWS KMS.
    """

    _key_arn: str
    _kms_client: Any
    _encryption_context: Optional[dict]

    def __init__(
        self,
        key_arn: str,
        encryption_context: Optional[dict] = None,
        boto3_session: Optional["boto3.Session"] = None,
    ):
        """
        Constructor.

        Args:
            key_arn: the ARN of the key to use for encryption (or the ARN of an alias).
            encryption_context: an encryption context of key/value pairs to be associated
                with each encryption operation.
            boto3_session: the AWS client library session. When not specified, the default
                session is used.
        """
        arn_parts = key_arn.split(":", 5)
        if not key_arn.startswith("arn:aws:kms:") or len(arn_parts) < 6:
            raise ValueError(f"Invalid AWS KMS key ARN {key_arn}")

        self._key_arn = key_arn
        self._encryption_context = encryption_context
        region_name = arn_parts[3]

        try:
            import boto3
        except ImportError as ex:
            raise Exception(
                "You are trying to encrypt connection credentials with an AWS KMS key, "
                "but the AWS client libraries could not be found. If you want to use this "
                "feature, please re-install the Gretel client with the [aws] option.",
            ) from ex

        if boto3_session is None:
            # not really a session, but all we need is the `client` method with the
            # right signature.
            boto3_session = boto3

        self._kms_client = boto3_session.client("kms", region_name=region_name)

    def _encrypt_payload(self, payload: bytes) -> bytes:
        encrypt_response = self._kms_client.encrypt(
            KeyId=self._key_arn,
            Plaintext=payload,
            EncryptionContext=self._encryption_context or {},
        )
        return encrypt_response["CiphertextBlob"]

    def make_encrypted_creds_config(self, ciphertext_b64: str) -> dict:
        return {
            "aws_kms": {
                "key_arn": self._key_arn,
                "encryption_context": self._encryption_context,
                "data": ciphertext_b64,
            }
        }
