"""
High level API for interacting with the Gretel Users API
"""

from typing import Optional

from gretel_client.config import ClientConfig, get_session_config
from gretel_client.rest.api.users_api import UsersApi


def get_me(as_dict: bool = True, *, session: Optional[ClientConfig] = None) -> dict:
    """
    Retrieve current user's profile from Gretel Cloud.

    Returns:
        A dictionary with the current user's profile information.

    Params:
        as_dict: If true, will return a raw dictionary of the user's data. This is currently
            the only option available.
        session: The client session to use, or ``None`` to use the default client
            session.
    """
    if session is None:
        session = get_session_config()
    api = session.get_api(UsersApi)
    resp = api.users_me()
    if as_dict:
        return resp.get("data", {}).get("me")

    raise NotImplementedError("Simple dict access to profile available only")
