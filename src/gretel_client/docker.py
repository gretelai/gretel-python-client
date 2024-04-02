"""
Helper classes for working with local docker containers
"""

from __future__ import annotations

import atexit
import io
import itertools
import signal
import tarfile
import uuid

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import urlparse

import docker
import docker.errors
import docker.models.containers
import smart_open

from docker.models.volumes import Volume
from docker.types.containers import DeviceRequest
from tqdm import tqdm
from tqdm.asyncio import tqdm as asyncio_tqdm

from gretel_client.config import ClientConfig, get_logger, get_session_config
from gretel_client.rest.api.opt_api import OptApi

DEFAULT_GPU_CONFIG = DeviceRequest(count=-1, capabilities=[["gpu"]])

CONTAINER_UTILS = "gretelai/container-utils"

SYSTEM_CHECK = "system-check"
GPU_SYSTEM_CHECK = "gpu-system-check"
VOLUME_BUILDER = "volume-builder"


class DockerError(Exception): ...


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
        self.current = round(self.current / 2**20) if self.current else None
        self.total = round(self.total / 2**20) if self.total else None

    @classmethod
    def from_dict(cls, source: dict) -> _PullUpdate:
        return cls(
            id=source.get("id", source.get("status")),
            status=source.get("status"),
            current=source.get("progressDetail", {}).get("current"),
            total=source.get("progressDetail", {}).get("total"),
        )

    @property
    def units(self) -> str:
        return "mb"

    def build_indicator(self) -> tqdm:
        params = {"total": self.total, "unit": self.units}
        # if we're in a notebook environment, ncols shouldn't
        # be configured. in a terminal environment tqdm
        # should be an instance of asyncio_tqdm.
        if tqdm == asyncio_tqdm:
            params["ncols"] = 80
        t = tqdm(**params)
        t.set_description(self.status)
        return t


class PullProgressPrinter:
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
    opt_api = session.get_api(OptApi)
    cred_resp = opt_api.get_container_login()
    return cred_resp.get("data").get("auth"), cred_resp.get("data").get("registry")


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
        raise DockerError(
            "Can't connect to docker. Please check that docker is installed and running."
        ) from ex


class AuthStrategy(Enum):
    AUTH = "auth"
    """Authenticate an already resolved container image."""

    AUTH_AND_RESOLVE = "auth-resolve"
    """Resolves a gretel container to an ECR registry and authenticates
    the image."""


@dataclass
class DataVolumeDef:
    target_dir: str
    """Defines container directory to place host file"""

    host_files: List[Tuple[str, Optional[str]]]
    """Specify what files to place into the container. The
    first item in the tuple is the host file and the second
    item optionally renames the host file on the container.
    """


class DataVolume:
    """Helper class for assembling a data volume.

    Using a data volume over a host mounted is preferable for a few reasons
        - Docker in docker container launches work as expected.
        - No file system permission issues to account for.

    Args:
        target_dir: Defines the container path to mount the volume to.
        docker_client: Reference to a docker client.
        volume_image: Specify the container image used to build the data
            volume.
    """

    DATA_VOLUME_PREFIX = "gretel"

    volume: Optional[Volume] = None
    volume_container: Optional[docker.models.containers.Container] = None

    @dataclass
    class File:
        source: str
        target: str

    def __init__(
        self,
        target_dir: str,
        docker_client: docker.DockerClient,
        volume_image: str = f"{CONTAINER_UTILS}:{VOLUME_BUILDER}",
    ):
        self.name = f"{self.DATA_VOLUME_PREFIX}-{uuid.uuid4().hex[:5]}"
        self.target_dir = target_dir
        self.docker_client = docker_client
        self.local_files: List[DataVolume.File] = []
        self.volume_image = volume_image
        atexit.register(self.cleanup)

    def add_file(self, local_file: Union[Path, str], target_file: str = None) -> str:
        if not isinstance(local_file, str):
            local_file = str(local_file)
        target_file = target_file or self._extract_file_name(local_file)
        self.local_files.append(DataVolume.File(local_file, target_file))
        return f"{self.target_dir}/{target_file}"

    def _extract_file_name(self, path: str) -> str:
        return Path(urlparse(path).path).name

    def copy_files(
        self,
        files: List[DataVolume.File],
        volume_container: docker.models.containers.Container,
    ):
        copy_stream = io.BytesIO()
        with tarfile.open(fileobj=copy_stream, mode="w") as tar_archive:
            for file in files:
                with smart_open.open(
                    file.source, "rb", ignore_ext=True
                ) as src:  # type:ignore
                    src.seek(0, 2)
                    info = tarfile.TarInfo(name=self._extract_file_name(file.target))
                    info.size = src.tell()
                    src.seek(0)
                    tar_archive.addfile(fileobj=src, tarinfo=info)  # type:ignore
            copy_stream.seek(0)
            volume_container.put_archive(data=copy_stream, path=self.target_dir)

    def prepare_volume(self, *, session: Optional[ClientConfig] = None) -> dict:
        if session is None:
            session = get_session_config()

        self.volume = self.docker_client.volumes.create(name=self.name)  # type:ignore
        image = pull_image(
            self.volume_image,
            self.docker_client,
            AuthStrategy.AUTH_AND_RESOLVE,
            session=session,
        )
        self.volume_container = self.docker_client.containers.create(  # type:ignore
            image=image,
            volumes=[f"{self.volume.name}:{self.target_dir}"],  # type: ignore
        )

        self.copy_files(self.local_files, self.volume_container)  # type:ignore
        return {self.name: {"bind": self.target_dir, "mode": "rw"}}

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


def pull_image(
    image: str,
    docker_client: docker.DockerClient,
    auth_strategy: Optional[AuthStrategy],
    *,
    session: Optional[ClientConfig] = None,
) -> str:
    if session is None:
        session = get_session_config()

    logger = get_logger(__name__)
    logger.debug("Authenticating image pull")

    auth = None
    if auth_strategy is not None:
        auth, registry = get_container_auth(session=session)
        if auth_strategy == AuthStrategy.AUTH_AND_RESOLVE:
            image = f"{registry}/{image}"
        elif auth_strategy == AuthStrategy.AUTH:
            parts = image.split("/", 1)
            # Override the registry from the authentication credentials
            # if the target registry is an AWS ECR registry.
            # TODO: is it safe to always override? We should only ever
            # be pulling Gretel images here.
            if len(parts) > 1 and parts[0].endswith(".amazonaws.com"):
                image = "/".join((registry, parts[1]))

    logger.info(f"Pulling container image {image}")
    try:
        pull = docker_client.api.pull(image, auth_config=auth, stream=True, decode=True)
        progress_printer = PullProgressPrinter(pull)
        progress_printer.start()

    except Exception as ex:
        raise DockerError(f"Could not pull image {image}") from ex

    return image


class Container:
    """Helper class for launching and supervising a Gretel Container.

    Args:
        img: The image to run
        auth_img: If set to ``True`` authenticate the docker image using
            Gretel Cloud credentials.
        params: A dictionary object of params to pass into the docker run
            command.
        files: A list of files to mount into the container. Notes these files
            will be written as a data volume.
    """

    def __init__(
        self,
        image: str,
        auth_strategy: Optional[AuthStrategy] = AuthStrategy.AUTH,
        params: Union[List[str], Dict[str, str]] = None,
        env: Dict[str, Optional[str]] = None,
        volumes: Union[dict, List[DataVolumeDef]] = None,
        device_requests: List = None,
        detach: bool = False,
        remove: bool = True,
        debug: bool = False,
    ):
        self.image = image
        self._auth_strategy = auth_strategy
        self._volumes = volumes or []
        self._device_requests = device_requests or []
        self._params = self._parse_params(params)
        self._env = [f"{k}={v}" for k, v in env.items() if v] if env else {}
        self._detach = detach
        self._docker_client = docker.from_env()
        self._logger = get_logger(__name__)
        self._debug = debug
        self._remove = remove
        self._run: Optional[docker.models.containers.Container] = None

    def _parse_params(
        self, params: Union[None, List[str], Dict[str, str]]
    ) -> List[str]:
        if isinstance(params, list):
            return params
        if isinstance(params, dict):
            return list(itertools.chain.from_iterable(params.items()))
        if params is None:
            return []

    def _prepare_volumes(
        self, volumes: Union[dict, List[DataVolumeDef]], *, session: ClientConfig
    ):
        if isinstance(volumes, dict):
            return volumes
        mounts = {}
        for vol_def in volumes:
            build = DataVolume(vol_def.target_dir, self._docker_client)
            for host_file, target_file in vol_def.host_files:
                build.add_file(host_file, target_file)
            mounts.update(build.prepare_volume(session=session))
        self._logger.debug(f"mounts {mounts}")
        return mounts

    def _start(self, image: str, entrypoint: str, volumes: dict):
        self._logger.info(f"Starting container {image}")
        run = self._docker_client.containers.run(
            image,
            self._params,
            entrypoint=entrypoint,
            detach=self._detach,
            volumes=volumes,
            environment=self._env,
            device_requests=self._device_requests,
            remove=self._remove,
        )
        atexit.register(self._cleanup)
        return run

    def _cleanup(self):
        """Remove the docker container"""
        if self._debug:
            return
        try:
            self.run.remove()
        except Exception:
            pass

    def logs(self) -> Iterator[str]:
        """Returns a log iterator from the runnering container."""
        for log in self.run.logs(stream=True):
            yield log.decode("utf-8").rstrip("\n")

    @property
    def run(self) -> docker.models.containers.Container:
        """Returns an instance of the running containers

        Throws:
            DockerError if the container hasn't been started.
        """
        if not self._run:
            raise DockerError("Container not running")
        return self._run

    def start(self, entrypoint: str = None, session: Optional[ClientConfig] = None):
        """Start the container"""
        if session is None:
            session = get_session_config()
        check_docker_env()
        image = pull_image(
            self.image, self._docker_client, self._auth_strategy, session=session
        )
        volumes = self._prepare_volumes(self._volumes, session=session)
        self._run = self._start(image, entrypoint, volumes)  # type:ignore

    def stop(self, force: bool = False):
        """If there is a running container this command will stop that
        container.

        Args:
            force: If force is ``True``, ``SIGKILL`` will be sent to the
                container, otherwise ``SIGTERM``.
        """
        try:
            self.run.kill(int(signal.SIGKILL if force else signal.SIGTERM))
        except Exception as e:
            self._logger.debug(f"Could not stop container {self} {e}")
        self._cleanup()

    def get_logs(self) -> str:
        """Return all logs from the container.

        Raises:
            :class:`~DockerError` if
            the logs can't be fetched. If the container has completed running,
            and is removed, errors can't be fetched and this error with be
            thrown.
        """
        try:
            return self.run.logs().decode("utf-8")
        except Exception as ex:
            raise DockerError(
                "Cannot get logs. The container may have been removed."
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
        try:
            self.run.reload()
            return self.run.status
        except (docker.errors.NotFound, DockerError):
            pass
        return "unknown"

    @property
    def exit_status(self) -> Optional[int]:
        try:
            self.run.reload()
            if self.run.status not in ["exited", "dead", "removing"]:
                return None
        except (docker.errors.NotFound, DockerError):
            return None
        return self.run.wait()["StatusCode"]


@dataclass
class CloudCreds(ABC):
    cred_from_agent: str

    @property
    @abstractmethod
    def volume(self) -> DataVolumeDef: ...

    @property
    @abstractmethod
    def env(self) -> Dict[str, str]: ...


class AwsCredFile(CloudCreds):
    base_dir: str = "/root/.aws"
    credential_file = "config"

    @property
    def volume(self) -> DataVolumeDef:
        return DataVolumeDef(
            "/root/.aws", [(self.cred_from_agent, self.credential_file)]
        )

    @property
    def env(self) -> Dict[str, str]:
        return {
            "AWS_CREDENTIAL_PROFILES_FILE": f"{self.base_dir}/{self.credential_file}",
            "AWS_DEFAULT_PROFILE": "default",
        }


class CaCertFile(CloudCreds):
    base_dir: str = "/etc/ssl"
    credential_file = "agent_ca.crt"

    @property
    def volume(self) -> DataVolumeDef:
        return DataVolumeDef(
            self.base_dir, [(self.cred_from_agent, self.credential_file)]
        )

    @property
    def env(self) -> Dict[str, str]:
        return {
            "REQUESTS_CA_BUNDLE": f"{self.base_dir}/{self.credential_file}",
        }


def extract_container_path(
    container: docker.models.containers.Container, container_path: str, host_path: str
):
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


def build_container(**kwargs) -> Container:
    """Factory method to build a container.

    See the ``Container`` constructor for a list valid kwargs.
    """
    return Container(**kwargs)


def check_gpu() -> bool:
    try:
        build_container(
            image=f"{CONTAINER_UTILS}:{SYSTEM_CHECK}",
            device_requests=[DEFAULT_GPU_CONFIG],
            remove=True,
            auth_strategy=AuthStrategy.AUTH_AND_RESOLVE,
        ).start()
        build_container(
            image=f"{CONTAINER_UTILS}:{GPU_SYSTEM_CHECK}",
            device_requests=[DEFAULT_GPU_CONFIG],
            params=["-c", "nvidia-smi"],
            remove=True,
            auth_strategy=AuthStrategy.AUTH_AND_RESOLVE,
        ).start(entrypoint="bash")
    except Exception as ex:
        get_logger(__name__).warn(str(ex))
        return False
    return True
