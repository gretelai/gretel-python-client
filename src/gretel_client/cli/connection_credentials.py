import base64
import re

from abc import ABC, abstractclassmethod
from pathlib import Path
from typing import Callable, Optional, Union

from gretel_client._hybrid.creds_encryption import CredentialsEncryption

_STD_B64_RE = re.compile(r"^[\sa-zA-Z0-9/+]+(?:=\s*){,2}$")
_URL_B64_RE = re.compile(r"^[\sa-zA-Z0-9_-]+(?:=\s*){,2}$")


class CredentialsEncryptionAdapter:

    _encryption_mechanism_ref: Union[
        CredentialsEncryption, Callable[[], CredentialsEncryption]
    ]
    """
    This can be a CredentialsEncryption instance, or a function for obtaining one.
    The latter enables lazy instantiation of the CredentialsEncryption object,
    allowing for higher-level errors to be reported before any credentials encryption
    configuration error.
    """

    _encrypted_creds_file: Optional[Path]

    def __init__(
        self,
        encryption_mechanism: Union[
            CredentialsEncryption, Callable[[], CredentialsEncryption]
        ],
        encrypted_credentials_file: Optional[Path],
    ):
        """
        Constructor.

        Args:
            encryption_mechanism: a CredentialsEncryption instance, or a zero-parameter
                function for obtaining one. The latter allows deferred instantiation
                for better controlling when errors are reported.
            encrypted_credentials_file: a file from which the encrypted credentials are read.
        """
        self._encryption_mechanism_ref = encryption_mechanism
        self._encrypted_creds_file = encrypted_credentials_file

    @property
    def _encryption_mechanism(self) -> CredentialsEncryption:
        # Allow lazy instantiation for deferred error condition checking.
        if not isinstance(
            self._encryption_mechanism_ref, CredentialsEncryption
        ) and callable(self._encryption_mechanism_ref):
            self._encryption_mechanism_ref = self._encryption_mechanism_ref()
        return self._encryption_mechanism_ref

    def apply(self, creds: Optional[dict]) -> dict:
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

            return self._encryption_mechanism.make_encrypted_creds_config(
                base64.b64encode(data).decode("ascii")
            )

        if self._encrypted_creds_file is not None:
            raise ValueError(
                "An encrypted credentials file must not be specified if "
                "the connection config contains plaintext credentials"
            )

        return self._encryption_mechanism.apply(creds)


class CredentialsEncryptionFlagsBase(ABC):
    @abstractclassmethod
    def _cli_decorate(cls, fn, param_name: str): ...

    @classmethod
    def options(cls, param_name: str):
        """
        Returns a decorator to add flags for the given encryption provider to a command function.

        Args:
            param_name: the name of the parameter of the function to be decorated
                receiving the optional provider instance.

        Returns:
            a decorator that adds encryption-related flags to a command function
            for a given provider, exposing them via a single optional
            ``CredentialsEncryption`` value.
        """

        def wrapper(fn):
            return cls._cli_decorate(fn, param_name)

        return wrapper
