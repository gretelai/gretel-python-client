from typing import Optional, Type, Union

from gretel_client.config import ClientConfig, configure_session, get_session_config
from gretel_client.navigator.client.interface import Client, ClientAdapter
from gretel_client.navigator.client.remote import RemoteClient


def get_navigator_client(
    client_adapter: Optional[Union[Type[ClientAdapter], ClientAdapter]] = None,
    session: Optional[ClientConfig] = None,
    **session_kwargs,
) -> Client:

    if session is None:
        configure_session(**session_kwargs)
        session = get_session_config()

    api_endpoint = "https://dataplane.gretel.cloud"
    if "api-dev" in session.endpoint:
        api_endpoint = "https://dataplane.dev.gretel.cloud"
    if any(token in session.endpoint for token in ["enterprise", "serverless"]):
        api_endpoint = session.endpoint

    if client_adapter is None:
        client_adapter = RemoteClient(api_endpoint, session)
    return Client(client_adapter)
