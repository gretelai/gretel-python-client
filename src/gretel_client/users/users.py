"""
High level API for interacting with the Gretel Users API
"""
from gretel_client.config import get_session_config
from gretel_client.rest.api.users_api import UsersApi


def get_me(as_dict: bool = True) -> dict:
    """
    Retrieve current user's profile from Gretel Cloud.

    Returns:
        A dictionary with the current user's profile information.

    Params:
        as_dict: If true, will return a raw dictionary of the user's data. This is currently
            the only option available.
    """
    api = get_session_config().get_api(UsersApi)
    resp = api.users_me()
    if as_dict:
        return resp.get("data", {}).get("me")

    raise NotImplementedError("Simple dict access to profile available only")
