from typing import Optional, Type, Union

from gretel_client.config import configure_session
from gretel_client.navigator.client.interface import Client, ClientAdapter
from gretel_client.navigator.client.remote import RemoteClient


def get_navigator_client(
    client_adapter: Optional[Union[Type[ClientAdapter], ClientAdapter]] = None,
    **session_kwargs,
) -> Client:
    if client_adapter is None:
        client_adapter = RemoteClient()
    if not isinstance(client_adapter, ClientAdapter):
        client_adapter = client_adapter()
    validate = session_kwargs.get("validate", False)
    configure_session(validate=validate, **session_kwargs)
    return Client(client_adapter)
