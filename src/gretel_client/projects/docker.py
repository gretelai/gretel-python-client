"""
Classes for running a Gretel Job as a local container
"""
from __future__ import annotations

import atexit
import io
import signal
import tarfile
import uuid

from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Dict, Iterator, List, Optional, Tuple, TYPE_CHECKING, Union
from urllib.parse import urlparse

import docker
import docker.errors
import smart_open

from docker.models.containers import Container
from docker.models.volumes import Volume
from docker.types.containers import DeviceRequest
from tqdm import tqdm

from gretel_client.config import get_logger, get_session_config
from gretel_client.projects.exceptions import ContainerRunError, DockerEnvironmentError
from gretel_client.projects.jobs import ACTIVE_STATES, Job
from gretel_client.rest.api.opt_api import OptApi

if TYPE_CHECKING:
    from gretel_client.projects.models import Model
else:
    Model = None


DEFAULT_ARTIFACT_DIR = "/workspace"


DEFAULT_GPU_CONFIG = DeviceRequest(count=-1, capabilities=[["gpu"]])


class DataVolume:

    volume: Optional[Volume] = None

    volume_container: Optional[Container] = None

    def __init__(self, host_dir: str, docker_client: docker.DockerClient):
        self.name = f"gretel-{uuid.uuid4().hex[:5]}"
        self.host_dir = host_dir
        self.docker_client = docker_client
        self.local_files = []
        self.volume_image = "busybox:latest"
        atexit.register(self.cleanup)

    def add_file(self, local_file: Union[Path, str]) -> str:
        if not isinstance(local_file, str):
            local_file = str(local_file)
        self.local_files.append(local_file)
        return f"{self.host_dir}/{self._extract_file_name(local_file)}"

    def _extract_file_name(self, path: str) -> str:
        return Path(urlparse(path).path).name

    def copy_files(self, files: List[str], volume_container: Container):
        copy_stream = io.BytesIO()
        with tarfile.open(fileobj=copy_stream, mode="w") as tar_archive:
            for file in files:
                with smart_open.open(file, "rb", ignore_ext=True) as src:  # type: ignore
                    src.seek(0, 2)
                    info = tarfile.TarInfo(name=self._extract_file_name(file))
                    info.size = src.tell()
                    src.seek(0)
                    tar_archive.addfile(fileobj=src, tarinfo=info)
            copy_stream.seek(0)
            volume_container.put_archive(data=copy_stream, path=self.host_dir)

    def prepare_volume(self) -> dict:
        self.volume = self.docker_client.volumes.create(name=self.name)  # type:ignore
        self.docker_client.images.pull(self.volume_image)
        self.volume_container = self.docker_client.containers.create(  # type:ignore
            image=self.volume_image,
            volumes=[f"{self.volume.name}:{self.host_dir}"],  # type: ignore
        )

        self.copy_files(self.local_files, self.volume_container)  # type:ignore
        return {self.name: {"bind": self.host_dir, "mode": "rw"}}

    def cleanup(self):
        if self.volume_container:
            try:
                self.volume_container.remove(force=True)
            except Exception:
                pass
        if self.volume:
            try:
                self.volume.remove(force=True)
            except Exception:
                pass


class ContainerRun:
    """Runs a Gretel Job from a local container.

    Args:
        job: Job to run as docker container.
    """

    image: str
    """The container image used for running the job"""

    model: Model
    """The model associated with the container run"""

    output_dir: Optional[Path]
    """Local file path to save artifacts to."""

    container_output_dir: Optional[str]
    """Output directory on the container where artifacts placed."""

    _docker_client: docker.DockerClient
    """Docker SDK instance"""

    _container: Optional[Container] = None
    """Reference to a running or completed container run."""

    def __init__(self, job: Job):
        check_docker_env()
        self._docker_client = docker.from_env()
        self.image = job.container_image
        self.input_volume = DataVolume("/in", self._docker_client)
        self.device_requests = []
        self.run_params = ["--disable-cloud-upload"]
        self.job = job
        self.configure_worker_token(job.worker_key)
        self.logger = get_logger(__name__)
        self.debug = False

    @classmethod
    def from_job(cls, job: Job) -> ContainerRun:
        job._poll_job_endpoint()
        return cls(job)

    def start(self):
        """Run job via a local container. This method
        is async and will return after the job has started.

        If you wish to block until the container has finished, the
        ``wait`` method may be used.
        """
        self._run(remove=self.debug)

    def extract_output_dir(self, dest: str):
        if not self.container_output_dir:
            return
        extract_container_path(self._container, self.container_output_dir, dest)

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

    def enable_cloud_uploads(self):
        self.run_params.remove("--disable-cloud-upload")

    def configure_gpu(self):
        try:
            self._check_gpu()
        except Exception as ex:
            raise ContainerRunError("GPU could not be configured") from ex
        self.device_requests.append(DEFAULT_GPU_CONFIG)

    def _check_gpu(self):
        if "synthetics" not in self.image:
            raise ContainerRunError("This image does not require a GPU")
        image = self._pull()
        self._docker_client.containers.run(
            image,
            entrypoint="bash",
            command=["-c", "nvidia-smi"],
            detach=False,
            remove=True,
            device_requests=[DEFAULT_GPU_CONFIG],
        )

    def stop(self, force: bool = False):
        """If there is a running container this command will stop that
        container.

        Args:
            force: If force is ``True``, ``SIGKILL`` will be sent to the
                container, otherwise ``SIGTERM``.
        """
        sig = signal.SIGKILL if force else signal.SIGTERM
        try:
            self._container.kill(int(sig))
        except Exception:
            pass

    def delete(self):
        """Remove the docker container"""
        if self.debug:
            return
        try:
            self._container.remove()
        except Exception:
            pass

    def _pull(self):
        self.logger.debug("Authenticating image pull")
        auth, _ = _get_container_auth()
        self.logger.info(f"Pulling container image {self.image}")
        try:
            pull = self._docker_client.api.pull(
                self.image, auth_config=auth, stream=True, decode=True
            )
            progress_printer = _PullProgressPrinter(pull)
            progress_printer.start()

        except Exception as ex:
            raise ContainerRunError(f"Could not pull image {self.image}") from ex
        return self.image

    def _run(self, remove: bool = True):
        image = self._pull()
        self.logger.debug("Preparing input data volume")
        volume_config = self.input_volume.prepare_volume()
        self._container = self._docker_client.containers.run(  # type:ignore
            image,
            self.run_params,
            detach=True,
            volumes=volume_config,
            device_requests=self.device_requests,
        )
        # ensure that the detached container stops when the process is closed
        atexit.register(self._cleanup)

    def get_logs(self) -> str:
        try:
            return self._container.logs().decode("utf-8")
        except Exception as ex:
            raise ContainerRunError(
                "Cannot get logs. Please re-run the job with debugging enabled."
            ) from ex

    @property
    def active(self) -> bool:
        """Returns ``True`` if the container is running. ``False`` otherwise."""
        return self.container_status not in {"exited", "dead", "unknown"}

    @property
    def container_status(self) -> Optional[str]:
        """Status from the running docker container.

        Valid statuses include:
            created, restarting, running, removing, paused, exited, or dead

        If the container isn't running, an "unknown" status will be
        returned.
        """
        if self._container:
            try:
                self._container.reload()
                return self._container.status
            except docker.errors.NotFound:
                pass
        return "unknown"

    def is_ok(self):
        """Checks to see if the container is ok.

        Raises:
            ``ContainerRunError`` if there is a problem with the container.
        """
        if self.job.status in ACTIVE_STATES and not self.active:
            try:
                self.logger.debug(self.get_logs())
            except Exception:
                pass
            if not self.debug:
                self.logger.warn("Re-run with debugging enabled for more details.")
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
        while self.active or cur < timeout:
            cur += 1
            sleep(1)

    def _cleanup(self):
        self.stop(force=True)
        self.delete()

    def graceful_shutdown(self):
        """Attempts to gracefully shutdown the container run."""
        try:
            self.job.cancel()
        except Exception:
            pass
        self.wait(15)


def _get_container_auth() -> Tuple[dict, str]:
    """Exchanges a Gretel Api Key for container registry credentials.

    Returns:
        An authentication object and registry endpoint. The authentication
        object may be passed into the docker sdk.
    """
    config = get_session_config()
    opt_api = config.get_api(OptApi)
    cred_resp = opt_api.get_container_login()
    return cred_resp.get("data").get("auth"), cred_resp.get("data").get("registry")


def extract_container_path(container: Container, container_path: str, host_path: str):
    """Writes all files from a container path to a host path"""
    stream = io.BytesIO()
    archive, _ = container.get_archive(container_path)
    for b in archive:
        stream.write(b)
    stream.seek(0)
    with tarfile.open(fileobj=stream, mode="r") as tar:
        dest_path = Path(host_path)
        dest_path.mkdir(exist_ok=True, parents=True)
        members_to_extact = []
        for member in tar.getmembers():
            if member.isfile():
                member.name = Path(member.name).name
                members_to_extact.append(member)
        tar.extractall(path=dest_path, members=members_to_extact)


def check_docker_env():
    """Checks that the local docker env is configured.

    Raises:
        ``DockerEnvironmentError`` if the docker environment isn't
        configured correctly.
    """
    try:
        client = docker.from_env()
        client.ping()
    except (docker.errors.APIError, docker.errors.DockerException) as ex:
        raise DockerEnvironmentError(
            "Can't connect to docker. Please check that docker is installed and running."
        ) from ex


@dataclass
class _PullUpdate:
    """The Docker daemon emits pull progress as a JSON
    schema. This dataclass is responsible for deserializing
    each JSON progress update from Docker.
    """

    id: str
    """Update id"""

    status: str
    """Update status"""

    current: Optional[int]
    """Units in mb"""

    total: Optional[int]
    """Units in mb"""

    def __post_init__(self):
        self.current = round(self.current / 2 ** 20) if self.current else None
        self.total = round(self.total / 2 ** 20) if self.total else None

    @classmethod
    def from_dict(cls, source: dict) -> _PullUpdate:
        return cls(
            id=source.get("id", source.get("status")),
            status=source["status"],
            current=source.get("progressDetail", {}).get("current"),
            total=source.get("progressDetail", {}).get("total"),
        )

    @property
    def units(self) -> str:
        return "mb"

    def build_indicator(self) -> tqdm:
        t = tqdm(total=self.total, unit=self.units, ncols=80)
        t.set_description(self.status)
        return t


class _PullProgressPrinter:
    """Print docker pull progress"""

    def __init__(self, pull: Iterator):
        self._pull = pull
        self._bars: Dict[str, tqdm] = {}

    def start(self):
        """Begin iterating and printing pull updates
        from the docker daemon.
        """
        for update in self._iter_updates():
            if update.current:
                self._update_progress(update)
        self._close_bars()

    def _close_bars(self):
        for bar in self._bars.values():
            bar.close()

    def _update_progress(self, update: _PullUpdate):
        bar = self._get_or_create_bar(update)
        self._update_bar_total(bar, update)

    def _get_or_create_bar(self, update: _PullUpdate) -> tqdm:
        if update.id in self._bars:
            return self._bars[update.id]
        else:
            self._bars[update.id] = update.build_indicator()
            return self._bars[update.id]

    def _update_bar_total(self, bar: tqdm, update: _PullUpdate):
        if bar.desc != update.status:
            bar.set_description(update.status)
        if update.current:
            bar.update(update.current - bar.n)

    def _iter_updates(self) -> Iterator[_PullUpdate]:
        for raw_update in self._pull:
            yield _PullUpdate.from_dict(raw_update)
