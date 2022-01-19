"""
Support for running local docker workers.
"""

from __future__ import annotations

from threading import Thread
from typing import TYPE_CHECKING

import docker

from gretel_client.agents.drivers.driver import Driver
from gretel_client.docker import build_container, Container

if TYPE_CHECKING:
    from gretel_client.agents.agent import AgentConfig, Job


class Docker(Driver):
    """Run a worker using a local Docker daemon.

    This driver is suitable for running Gretel Workers on local computers or
    VMs where a docker daemon is running.
    """

    def __init__(self):
        self._docker_client = docker.from_env()

    @classmethod
    def from_config(cls, config: AgentConfig) -> Docker:
        return cls()

    def schedule(self, job: Job) -> Container:
        volumes = []
        if job.cloud_creds:
            volumes.append(job.cloud_creds.volume)
        container_run = build_container(
            image=job.container_image, params=job.params, env=job.env, volumes=volumes
        )
        container_run.start()
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
