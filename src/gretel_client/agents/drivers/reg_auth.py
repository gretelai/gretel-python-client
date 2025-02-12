"""
Agent Driver Registry
"""

from __future__ import annotations

from typing import Optional, Tuple

from gretel_client.config import ClientConfig, get_session_config
from gretel_client.rest.api.opt_api import OptApi


def get_container_auth(*, session: Optional[ClientConfig] = None) -> Tuple[dict, str]:
    """Exchanges a Gretel Api Key for container registry credentials.

    Args:
        session: The client session to use, or ``None`` to use the default session.
    Returns:
        An authentication object and registry endpoint. The authentication
        object may be passed into the docker sdk.
    """
    if session is None:
        session = get_session_config()
    opt_api: OptApi = session.get_api(OptApi)
    cred_resp: dict = opt_api.get_container_login()
    return cred_resp.get("data").get("auth"), cred_resp.get("data").get("registry")
