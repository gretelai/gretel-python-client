"""
Classes for running a Gretel Job as a local container
"""
import atexit
import signal
import tempfile
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Optional, Tuple

import docker
import smart_open
from docker.models.containers import Container

from gretel_client_v2.config import get_session_config
from gretel_client_v2.rest.api.opt_api import OptApi

if TYPE_CHECKING:
    from gretel_client_v2.projects.models import Model
else:
    Model = None


class ContainerRunError(Exception):
    ...


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
        self, model: Model, output_dir: str = None, disable_uploads: bool = False
    ):
        self._docker_client = docker.from_env()
        self.image = f"gretelai/{model.model_type}:dev"  # todo(dn): consolidate image logic onto api.
        self.model = model
        self.output_dir = Path(output_dir).resolve() if output_dir else None
        self.temp_data_source = None
        self.disable_uploads = disable_uploads
        self._launched_from_docker = _is_inside_container()

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

    def stop(self, force: bool = False):
        """If there is a running container this command will stop that
        container.

        Args:
            force: If force is ``True``, ``SIGKILL`` will be sent to the
                container, otherwise ``SIGINT``.
        """
        sig = signal.SIGKILL if force else signal.SIGINT
        try:
            self._container.kill(int(sig))
        except Exception:
            pass

    def _setup_volumes(self) -> dict:
        """Configures volumes to mount into the container.

        Note: If the CLI is being run from inside a container this command
        will return an empty volume configuration. Mounting volumes from
        inside an already runningcontainer is problematic.
        """
        if self._launched_from_docker:
            return {}
        volumes = {}
        if self.output_dir:
            volumes[str(self.output_dir)] = {
                "bind": self.container_artifact_dir,
                "mode": "rw",
            }
        if self.model.external_data_source:
            self.temp_data_source = tempfile.NamedTemporaryFile()
            with smart_open.open(self.model.data_source, "rb") as src:
                self.temp_data_source.write(src.read())
            volumes[self.temp_data_source.name] = {
                "bind": self.temp_data_source.name,
                "mode": "rw",
            }

        return volumes

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
        volumes = self._setup_volumes()
        args = [
            "--worker-token",
            self.model._worker_key,
            "--artifact-dir",
            self.container_artifact_dir,
        ]
        if self.temp_data_source:
            args.extend(["--data-source", self.temp_data_source.name])
        if self.disable_uploads:
            args.append("--disable-cloud-upload")
        self._container = self._docker_client.containers.run(
            image,
            args,
            detach=True,
            remove=remove,
            volumes=volumes,
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
