"""
Classes for running a Gretel Job as a local container
"""
from __future__ import annotations

import atexit
import shutil
import signal
import tempfile
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, List, Optional, Tuple, Union
from urllib.parse import urlparse

import docker
import docker.errors
import smart_open
from docker.models.containers import Container
from docker.types.containers import DeviceRequest

from gretel_client_v2.config import get_session_config
from gretel_client_v2.projects.records import RecordHandler
from gretel_client_v2.rest.api.opt_api import OptApi

if TYPE_CHECKING:
    from gretel_client_v2.projects.models import Model
else:
    Model = None


class ContainerRunError(Exception):
    ...


DEFAULT_ARTIFACT_DIR = "/workspace"


DEFAULT_GPU_CONFIG = DeviceRequest(count=-1, capabilities=[['gpu']])


class VolumeBuilder:

    def __init__(self):
        self.volumes = {}
        self.input_mappings = {}
        self.cleanup_paths = []
        atexit.register(self.cleanup)

    def add_output_volume(self, host_dir: Optional[str], container_dir: Optional[str]):
        if host_dir:
            host_dir_qualified = str(Path(host_dir).resolve())
            self.volumes[host_dir_qualified] = {"bind": container_dir, "mode": "rw"}

    def add_input_volume(
        self, container_dir: str, files: List[Tuple[str, Optional[str]]] = []
    ):
        host_tmp_dir = tempfile.mkdtemp()
        for file_key, host_file_path in files:
            if not host_file_path:
                continue
            host_file_name = Path(urlparse(host_file_path).path).name
            with smart_open.open(host_file_path, "rb", ignore_ext=True) as src:  # type: ignore
                tmp_host_file = Path(host_tmp_dir) / host_file_name
                with open(tmp_host_file, "wb") as out:
                    out.write(src.read())  # type:ignore
            self.input_mappings[file_key] = f"{container_dir}/{host_file_name}"
        self.volumes[host_tmp_dir] = {"bind": container_dir, "mode": "rw"}
        self.cleanup_paths.append(host_tmp_dir)

    def cleanup(self):
        for path in self.cleanup_paths:
            shutil.rmtree(path, ignore_errors=True)


class ContainerRun:
    """Runs a Gretel Job from a local container.

    Args:
        model: Model to run.
        output_dir: Local file path to save run artifacts to. If no path
            is specified the artifacts will remain on the container.
    """

    image: str
    """The container image used for running the job"""

    model: Model
    """The model associated with the container run"""

    output_dir: Optional[Path]
    """Local file path to save artifacts to."""

    container_artifact_dir: str = "/workspace"
    """Output directory on the container where artifacts placed."""

    _docker_client: docker.DockerClient
    """Docker SDK instance"""

    _container: Optional[Container] = None
    """Reference to a running or completed container run."""

    def __init__(
        self,
        image: str,
        job: Union[Model, RecordHandler]  # todo(dn): consolidate into a single parent class
    ):
        self._docker_client = docker.from_env()
        self.image = image
        self.volumes = VolumeBuilder()
        self.device_requests = []
        self.run_params = ["--disable-cloud-upload"]
        self.job = job
        self._launched_from_docker = _is_inside_container()

    @classmethod
    def from_record_handler(cls, record_handler: RecordHandler) -> ContainerRun:
        cr = cls(f"gretelai/{record_handler.type}:dev", record_handler)
        if record_handler.worker_key:
            cr.configure_worker_token(record_handler.worker_key)
        return cr

    @classmethod
    def from_model(cls, model: Model) -> ContainerRun:
        cr = cls(f"gretelai/{model.model_type}:dev", model)
        if model._worker_key:
            cr.configure_worker_token((model._worker_key))
        return cr

    def start(self, _debug: bool = False):
        """Run job via a local container. This method
        is async and will return after the job has started.

        If you wish to block until the container has finished, the
        ``wait`` method may be used.

        Args:
            _debug: If ``_debug`` is set to ``True`` the container won't be
                removed after the job has finished running. This is useful
                if you need to inspect container logs.
        """
        self._run(remove=not _debug)

    def configure_worker_token(self, worker_token: str):
        self.run_params.extend(["--worker-token", worker_token])

    def configure_output_dir(
        self, host_dir: str, container_dir: str = DEFAULT_ARTIFACT_DIR
    ):
        self.volumes.add_output_volume(host_dir, container_dir)
        self.run_params.extend(["--artifact-dir", container_dir])

    def configure_model(self, model_path: str):
        if not isinstance(model_path, str):
            model_path = str(model_path)
        self.volumes.add_input_volume("/model", [("model", model_path)])
        in_model_path = self.volumes.input_mappings["model"]
        self.run_params.extend(["--model-path", in_model_path])

    def configure_input_data(self, input_data: str):
        if not isinstance(input_data, str):
            input_data = str(input_data)
        self.volumes.add_input_volume("/in", [("in_data", input_data)])
        in_data_path = self.volumes.input_mappings["in_data"]
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
            device_requests=[DEFAULT_GPU_CONFIG]
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

    def _pull(self):
        auth, reg = _get_container_auth()
        img = f"{reg}/{self.image}"
        try:
            self._docker_client.images.pull(img, auth_config=auth)
        except Exception as ex:
            raise ContainerRunError(f"Could not pull image {img}") from ex
        return img

    def _run(self, remove: bool = True):
        image = self._pull()
        self._container = self._docker_client.containers.run(  # type:ignore
            image,
            self.run_params,
            detach=True,
            remove=remove,
            volumes=self.volumes.volumes,
            device_requests=self.device_requests,
        )
        # ensure that the detached container stops when the process is closed
        atexit.register(self._cleanup)

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

    def graceful_shutdown(self):
        try:
            self.job.cancel()
        except Exception:
            pass
        self.wait(15)


def _is_inside_container() -> bool:
    """Returns ``True`` if the function is being run inside a container."""
    try:
        with open("/proc/self/cgroup") as fin:
            for line in fin:
                parts = line.split("/")
                sha = parts[-1].rstrip()
                if len(sha) == 64:
                    return True
    except FileNotFoundError:
        pass
    return False


def _get_container_auth() -> Tuple[dict, str]:
    """Exchanges a Gretel Api Key for container registry credentials.

    Returns:
        An authentication object that may be passed
        into the docker api, and the registry endpoint.
    """
    config = get_session_config()
    opt_api = config.get_api(OptApi)
    cred_resp = opt_api.get_container_login()
    return cred_resp.get("data").get("auth"), cred_resp.get("data").get("registry")
