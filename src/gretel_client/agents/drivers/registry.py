"""
Agent Driver Registry
"""

from __future__ import annotations

from typing import Dict, TYPE_CHECKING

from gretel_client.agents.drivers.driver import Driver, DriverError
from gretel_client.agents.drivers.k8s import Kubernetes
from gretel_client.rest.api.opt_api import OptApi

if TYPE_CHECKING:
    from gretel_client.agents.agent import AgentConfig


_registry: Dict[str, type[Driver]] = {"k8s": Kubernetes}
"""New drivers that implement ``Driver`` should be added to this registry"""


def get_driver(config: AgentConfig) -> Driver:
    """Returns a driver from the registry based on the ``AgentConfig``."""
    factory = _registry.get(config.driver)
    if not factory:
        raise DriverError(f"No driver found for `{config.driver}`")
    return factory.from_config(config)
