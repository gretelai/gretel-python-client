import base64
import os
import re

from typing import Optional

import yaml

from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

from gretel_client._hybrid.creds_encryption import BaseCredentialsEncryption
from gretel_client.rest_v1.api.projects_api import ProjectsApi
from gretel_client.rest_v1.models.config_asymmetric_key_metadata import (
    ConfigAsymmetricKeyMetadata,
)


class AsymmetricCredentialsEncryption(BaseCredentialsEncryption):

    _projects_api: Optional[ProjectsApi]
    _asymmetric_key_metadata: Optional[ConfigAsymmetricKeyMetadata]

    def __init__(
        self,
        *,
        projects_api: Optional[ProjectsApi] = None,
        asymmetric_key_metadata: Optional[ConfigAsymmetricKeyMetadata] = None,
    ):
        self._projects_api = projects_api
        self._asymmetric_key_metadata = asymmetric_key_metadata

    def apply(self, credentials: dict, *, project_guid: Optional[str] = None):
        asymmetric_key = self._asymmetric_key_metadata
        if not asymmetric_key:
            if not project_guid:
                raise ValueError(
                    "can not apply asymmetric encryption for connections not specifying a project ID"
                )
            if not self._projects_api:
                raise ValueError(
                    "encryption mechanism is not configured for dynamic retrieval of asymmetric key"
                )

            project = self._projects_api.get_project(
                project_guid, expand=["cluster"]
            ).project
            if not (cluster := project.cluster):
                raise ValueError(
                    f"project {project_guid} is not a hybrid project, or does not have a hybrid cluster associated with it"
                )
            if not cluster.config or not cluster.config.asymmetric_key:
                raise ValueError(
                    f"cluster {cluster.guid} for project {project_guid} does not have asymmetric encryption enabled"
                )

            asymmetric_key = cluster.config.asymmetric_key

        return _encrypt_asymmetric(credentials, asymmetric_key)


def _encrypt_symmetric(data: bytes) -> tuple[bytes, bytes]:
    client_key = os.urandom(32)
    nonce = os.urandom(12)

    symmetric_cipher = AES.new(client_key, mode=AES.MODE_GCM, nonce=nonce, mac_len=16)
    creds_encrypted, tag = symmetric_cipher.encrypt_and_digest(data)

    ciphertext = nonce + creds_encrypted + tag
    return ciphertext, client_key


def _encrypt_asymmetric(
    credentials: dict, asymmetric_key: ConfigAsymmetricKeyMetadata
) -> dict:
    if (algo := asymmetric_key.algorithm) != "RSA_4096_OAEP_SHA256":
        raise ValueError(f"unsupported algorithm {algo}")

    public_key = RSA.import_key(asymmetric_key.public_key_pem)
    asymmetric_cipher = PKCS1_OAEP.new(public_key, hashAlgo=SHA256)

    creds_yaml = yaml.dump(credentials).encode("utf-8")

    ciphertext, client_key = _encrypt_symmetric(creds_yaml)
    ciphertext_b64 = base64.b64encode(ciphertext).decode("ascii")

    client_key_encrypted = asymmetric_cipher.encrypt(client_key)
    client_key_encrypted_b64 = base64.b64encode(client_key_encrypted).decode("ascii")

    return {
        "asymmetric": {
            "key_id": asymmetric_key.key_id,
            "algorithm": algo,
            "encrypted_client_key": client_key_encrypted_b64,
            "data": ciphertext_b64,
        }
    }
