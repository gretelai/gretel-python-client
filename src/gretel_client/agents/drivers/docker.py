"""
Support for running local docker workers.
"""

from __future__ import annotations

from threading import Thread
from typing import TYPE_CHECKING

import docker

from gretel_client.agents.drivers.driver import Driver, GPU
from gretel_client.config import ClientConfig, get_logger
from gretel_client.docker import build_container, Container, DEFAULT_GPU_CONFIG

if TYPE_CHECKING:
    from gretel_client.agents.agent import AgentConfig, Job


class Docker(Driver):
    """Run a worker using a local Docker daemon.

    This driver is suitable for running Gretel Workers on local computers or
    VMs where a docker daemon is running.
    """

    def __init__(self, agent_config: AgentConfig):
        self._docker_client = docker.from_env()
        self._agent_config = agent_config
        self._logger = get_logger(__name__)

    @classmethod
    def from_config(cls, agent_config: AgentConfig) -> Docker:
        return cls(agent_config)

    @property
    def session(self) -> ClientConfig:
        return self._agent_config.session

    def schedule(self, job: Job) -> Container:
        volumes = []
        if job.cloud_creds:
            for cred in job.cloud_creds:
                volumes.append(cred.volume)

        if self._agent_config.volumes:
            for vol in self._agent_config.volumes:
                volumes.append(vol)

        device_requests = []
        if job.needs_gpu:
            if (
                self._agent_config.capabilities
                and GPU in self._agent_config.capabilities
            ):
                device_requests.append(DEFAULT_GPU_CONFIG)
            else:
                self._logger.warn("This job requires a GPU but no GPU is configured")

        container_run = build_container(
            image=job.container_image,
            params=job.params,
            env=job.env,
            volumes=volumes,
            device_requests=device_requests,
            detach=True,
        )
        container_run.start(session=self.session)
        if job.log:

            def print_logs():
                for log in container_run.logs():
                    job.log(log)  # type:ignore

            log_printer = Thread(target=print_logs)
            log_printer.start()

        return container_run

    def clean(self, container: Container):
        container.stop()

    def shutdown(self, container: Container):
        container.stop()

    def active(self, container: Container) -> bool:
        return container.active

    def has_errored(self, unit: Container) -> bool:
        return unit.exit_status != 0
