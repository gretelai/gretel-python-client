from typing import Any

from pydantic import StrictStr

from gretel_client._hybrid.creds_encryption import CredentialsEncryption
from gretel_client.rest_v1.api.connections_api import (
    ConnectionsApi,
    CreateConnectionRequest,
    UpdateConnectionRequest,
)
from gretel_client.rest_v1.api_response import ApiResponse


class HybridConnectionsApi(ConnectionsApi):
    """
    Hybrid wrapper for the connections api.

    Objects of this class behave like the regular connections API,
    with the following exceptions:
    - if a connection is attempted to be created with plaintext credentials,
      these will be encrypted with the specified credentials encryption mechanism
      before making the request to the server.
    - if a connection is attempted to be updated with plaintext credentials,
      these will be encrypted with the specified credentials encryptuon mechanism
      before making the request to the server.
    """

    _creds_encryption: CredentialsEncryption

    def __init__(self, api: ConnectionsApi, creds_encryption: CredentialsEncryption):
        """
        Constructor.

        Args:
            api: the regular connections API object.
            creds_encryption: a credentials encryption mechanism, that is used to
                securely encrypted credentials for storage on Gretel's server.
        """
        super().__init__(api.api_client)
        self._creds_encryption = creds_encryption

    def create_connection_with_http_info(
        self, create_connection_request: CreateConnectionRequest, **kwargs
    ) -> ApiResponse:
        if creds := create_connection_request.credentials:
            create_connection_request.credentials = None
            encrypted_creds = self._creds_encryption.apply(creds)
            create_connection_request.encrypted_credentials = encrypted_creds

        return super().create_connection_with_http_info(
            create_connection_request, **kwargs
        )

    def update_connection_with_http_info(
        self,
        connection_id: StrictStr,
        update_connection_request: UpdateConnectionRequest,
        **kwargs,
    ) -> ApiResponse:
        if creds := update_connection_request.credentials:
            update_connection_request.credentials = None
            encrypted_creds = self._creds_encryption.apply(
                creds,
            )
            update_connection_request.encrypted_credentials = encrypted_creds

        return super().update_connection_with_http_info(
            connection_id, update_connection_request, **kwargs
        )
