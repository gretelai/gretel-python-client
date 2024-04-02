"""
Base class for an Agent driver.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from gretel_client.agents.agent import AgentConfig, Job


class DriverError(Exception): ...


GPU = "gpu"

ComputeUnit = TypeVar("ComputeUnit")
"""Defines the underlying compute unit for the driver."""


class Driver(ABC, Generic[ComputeUnit]):
    """A driver implements a set of methods that may be used to
    launch Gretel Workers on various container platforms such as
    Docker, ECS or k8s.

    Each driver returns a Generic compute unit, ``U`` that is used for
    managing the lifecycle of each driver's compute resource. For docker, the
    unit might be a container, ECS a task definition and k8s a job.
    """

    @classmethod
    @abstractmethod
    def from_config(cls, agent_config: AgentConfig) -> Driver:
        """Instantiate a driver from an ``AgentConfig``."""
        ...

    @abstractmethod
    def schedule(self, job: Job) -> ComputeUnit:
        """Schedule a job for execution.

        Returns:
            A compute unit ``ComputeUnit``.
        """
        ...

    @abstractmethod
    def active(self, unit: ComputeUnit) -> bool:
        """Return ``True`` if the compute unit is active and running."""
        ...

    @abstractmethod
    def has_errored(self, unit: ComputeUnit) -> bool:
        """Return ``True`` if the job ended in an error state."""
        ...

    @abstractmethod
    def shutdown(self, unit: ComputeUnit):
        """Terminates the compute unit."""
        ...

    @abstractmethod
    def clean(self, unit: ComputeUnit):
        """Cleans up resources created by the compute unit."""
        ...
