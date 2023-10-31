import base64
import json
import re

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Type

_STD_B64_RE = re.compile(r"^[\sa-zA-Z0-9/+]+(?:=\s*){,2}$")
_URL_B64_RE = re.compile(r"^[\sa-zA-Z0-9_-]+(?:=\s*){,2}$")


class CredentialsEncryption(ABC):
    _encrypted_creds_file: Optional[Path]

    def __init__(self, encrypted_credentials_file: Optional[Path]):
        self._encrypted_creds_file = encrypted_credentials_file

    @classmethod
    @abstractmethod
    def cli_decorate(cls, fn, param_name: str):
        """
        Decorates a CLI entrypoint with flags pertaining to this
        encryption provider implementation.

        Args:
            param_name: the name of the parameter receiving the (optional)
                instance of the respective encryption provider.

        Returns:
            A decorated version of fn.
        """
        ...

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
    def _make_encrypted_creds_config(self, ciphertext: bytes) -> dict:
        """
        Creates an `encrypted_credentials` configuration dictionary.

        Args:
            ciphertext: the encrypted credentials ciphertext.

        Returns:
            a dictionary that can be used as the `encrypted_credentials`
            section in a connection config, incorporating the given
            ``ciphertext`` plus metadata.
        """
        ...

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
        ciphertext = self._get_ciphertext(credentials)
        return self._make_encrypted_creds_config(ciphertext)

    def _get_ciphertext(self, creds: Optional[dict]) -> bytes:
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

        creds_json = json.dumps(creds)
        return self._encrypt_payload(creds_json.encode("utf-8"))


def encryption_flags(cls: Type[CredentialsEncryption], param_name: str):
    """
    Returns a decorator to flags for the given encryption provider to a command function.

    Args:
        param_name: the name of the parameter of the function to be decorated
            receiving the optional provider instance.

    Returns:
        a decorator that adds encryption-related flags to a command function
        for a given provider, exposing them via a single optional
        ``CredentialsEncryption`` value.
    """

    def wrapper(fn):
        return cls.cli_decorate(fn, param_name)

    return wrapper
