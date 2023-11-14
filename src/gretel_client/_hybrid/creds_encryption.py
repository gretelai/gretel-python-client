import base64

from abc import ABC, abstractmethod
from typing import TypeVar, Union

import yaml

from gretel_client.rest_v1.api.connections_api import (
    Connection,
    CreateConnectionRequest,
    UpdateConnectionRequest,
)

T = TypeVar(
    "T", bound=Union[dict, Connection, CreateConnectionRequest, UpdateConnectionRequest]
)


class CredentialsEncryption(ABC):
    """
    Abstract base class for connection credential encyrption mechanisms.
    """

    @abstractmethod
    def _encrypt_payload(self, payload: bytes) -> bytes:
        """
        Encrypts the given payload using the provider-specific
        encryption mechanism.

        Args:
            payload: the encryption payload. This will always be
                a serialized form of the plaintext credentials.

        Returns:
            the encrypted ciphertext as a bytes object.
        """
        ...

    @abstractmethod
    def make_encrypted_creds_config(self, ciphertext_b64: str) -> dict:
        """
        Creates an `encrypted_credentials` configuration dictionary.

        Args:
            ciphertext_b64: the encrypted credentials ciphertext, base64
                encoded.

        Returns:
            a dictionary that can be used as the `encrypted_credentials`
            section in a connection config, incorporating the given
            ``ciphertext`` plus metadata.
        """
        ...

    def apply(self, credentials: dict) -> dict:
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
        creds_yaml = yaml.dump(credentials)
        ciphertext = self._encrypt_payload(
            creds_yaml.encode("utf-8"),
        )
        return self.make_encrypted_creds_config(
            base64.b64encode(ciphertext).decode("ascii"),
        )

    def apply_connection(self, connection: T) -> T:
        """
        Convenience method that applies credentials encryption to an entire connection.

        The passed connection can be a ``dict`` or any of the credentials-containing
        connection representations from the workflows API, i.e., a ``Connection``,
        ``CreateConnectionRequest``, or ``UpdateConnectionRequest``.

        Args:
            connection: the connection dict or object.

        Returns:
            an object of the same type, with credentials encryption applied.
        """
        connection_dict = (
            connection if isinstance(connection, dict) else connection.to_dict()
        )
        if plaintext_creds := connection_dict.pop("credentials", None):
            encrypted_creds = self.apply(plaintext_creds)
            connection_dict["encrypted_credentials"] = encrypted_creds
        return (
            connection_dict
            if isinstance(connection, dict)
            else connection.from_dict(connection_dict)
        )


class NoCredentialsEncryption(CredentialsEncryption):
    def _encrypt_payload(self, payload: bytes) -> bytes:
        raise NotImplementedError("no credentials encryption is configured")

    def make_encrypted_creds_config(self, ciphertext_b64: str) -> dict:
        raise NotImplementedError("no credentials encryption is configured")
