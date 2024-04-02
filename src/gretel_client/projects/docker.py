"""
Classes for running a Gretel Job as a local container
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Union

import docker
import docker.errors
import docker.models.containers

from gretel_client.cli.utils.parser_utils import ref_data_factory
from gretel_client.config import ClientConfig, get_logger
from gretel_client.docker import (
    build_container,
    check_docker_env,
    Container,
    DataVolume,
    DEFAULT_GPU_CONFIG,
    extract_container_path,
)
from gretel_client.models.config import get_model_type_config
from gretel_client.projects.exceptions import ContainerRunError
from gretel_client.projects.jobs import ACTIVE_STATES, Job
from gretel_client.projects.models import Model
from gretel_client.projects.records import RecordHandler

if TYPE_CHECKING:
    from gretel_client.cli.utils.parser_utils import RefData

DEFAULT_ARTIFACT_DIR = "/workspace"


class ContainerRun:
    """Runs a Gretel Job from a local container.

    Args:
        job: Job to run as docker container.
    """

    def __init__(self, job: Job):
        check_docker_env()

        self._docker_client = docker.from_env()
        self.image = job.container_image
        self.input_volume = DataVolume("/in", self._docker_client)
        self.device_requests = []
        self.run_params = ["--disable-cloud-upload"]
        self.job = job
        self._container = None
        self.logger = get_logger(__name__)
        self.debug = False

        if job.worker_key:
            self.configure_worker_token(job.worker_key)
        else:
            raise ContainerRunError("No worker token provided")

    @classmethod
    def from_job(cls, job: Job) -> ContainerRun:
        job._poll_job_endpoint()
        return cls(job)

    @property
    def session(self) -> ClientConfig:
        return self.job.session

    def start(self):
        """Run job via a local container. This method
        is async and will return after the job has started.

        If you wish to block until the container has finished, the
        ``wait`` method may be used.
        """
        self._run()

    def extract_output_dir(self, dest: str):
        if not self.container_output_dir:
            return
        extract_container_path(self.container.run, self.container_output_dir, dest)

    def enable_debug(self):
        self.debug = True

    def configure_worker_token(self, worker_token: str):
        self.run_params.extend(["--worker-token", worker_token])

    def configure_output_dir(
        self, host_dir: str, container_dir: str = DEFAULT_ARTIFACT_DIR
    ):
        self.host_dir = host_dir
        self.container_output_dir = container_dir
        self.run_params.extend(["--artifact-dir", container_dir])

    def configure_model(self, model_path: Union[str, Path]):
        if not isinstance(model_path, str):
            model_path = str(model_path)
        in_model_path = self.input_volume.add_file(model_path)
        self.run_params.extend(["--model-path", in_model_path])

    def configure_input_data(self, input_data: Union[str, Path]):
        if not isinstance(input_data, str):
            input_data = str(input_data)
        in_data_path = self.input_volume.add_file(input_data)
        self.run_params.extend(["--data-source", in_data_path])

    def configure_ref_data(self, ref_data: Union[dict, RefData]):
        if isinstance(ref_data, dict):
            for key, path in ref_data.items():
                ref_data[key] = str(path)
            ref_data = ref_data_factory(ref_data)

        # Make a deep copy in the event we have to re-write the
        # values of the ref data dict for the new input volume paths
        ref_data = deepcopy(ref_data)

        # We only configure ref data if all data sources are local to disk
        if ref_data.is_local_data:
            for key, data_path in ref_data.ref_dict.items():
                ref_data.ref_dict[key] = self.input_volume.add_file(data_path)
            self.run_params.extend(ref_data.as_cli)

    def enable_cloud_uploads(self):
        self.run_params.remove("--disable-cloud-upload")

    def configure_gpu(self):
        try:
            self._check_gpu()
        except Exception as ex:
            self.logger.debug("Error running GPU check container: %s" % str(ex))
            raise ContainerRunError("GPU could not be configured") from ex
        self.device_requests.append(DEFAULT_GPU_CONFIG)

    def _check_gpu(self):
        model_type_config = get_model_type_config(self.job.model_type)
        if isinstance(self.job, Model):
            if model_type_config.train_instance_type != "gpu":
                raise ContainerRunError("This image does not require a GPU")
        elif isinstance(self.job, RecordHandler):
            if model_type_config.run_instance_type != "gpu":
                raise ContainerRunError("This image does not require a GPU")

        build_container(
            image=self.image,
            device_requests=[DEFAULT_GPU_CONFIG],
            params=["-c", "nvidia-smi"],
            remove=True,
        ).start(entrypoint="bash", session=self.session)

    def _run(self):
        self.logger.debug("Preparing input data volume")
        volumes = self.input_volume.prepare_volume(session=self.session)
        self._container = build_container(
            image=self.image,
            params=self.run_params,
            detach=True,
            volumes=volumes,
            remove=False,
            env={"GRETEL_STAGE": self.session.stage},
            device_requests=self.device_requests,
        )
        self._container.start(session=self.session)

    @property
    def container(self) -> Container:
        if not self._container:
            raise ContainerRunError("Trying to access a container that isn't running")
        else:
            return self._container

    def is_ok(self):
        """Checks to see if the container is ok.

        Raises:
            ``ContainerRunError`` if there is a problem with the container.
        """
        if self.job.status in ACTIVE_STATES and not self.container.active:
            try:
                self.logger.debug(self.container.get_logs())
            except Exception:
                pass
            if not self.debug:
                self.logger.warning("Re-run with debugging enabled for more details.")
            raise ContainerRunError(
                ("Could not launch container. Please check the logs for more details.")
            )

    def wait(self, timeout: int = 30):
        """Blocks until a running container has completed. If the
        container hasn't started yet, we wait until a ``timeout``
        interval is reached.

        Args:
            timeout: The time in seconds to wait for a container
                to start. If the timeout is reached, the function will
                return.
        """
        cur = 0
        while self.container.active or cur < timeout:
            cur += 1
            sleep(1)

    def graceful_shutdown(self):
        """Attempts to gracefully shutdown the container run."""
        try:
            self.job.cancel()
        except Exception:
            pass
        self.wait(15)
