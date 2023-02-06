from __future__ import annotations

import json
import logging
import os

from enum import Enum
from getpass import getpass
from pathlib import Path
from typing import Optional, Type, TypeVar, Union

import certifi

from urllib3.util import Retry

from gretel_client.rest.api.projects_api import ProjectsApi
from gretel_client.rest.api.users_api import UsersApi
from gretel_client.rest.api_client import ApiClient
from gretel_client.rest.configuration import Configuration
from gretel_client.rest_v1.api_client import ApiClient as V1ApiClient
from gretel_client.rest_v1.configuration import Configuration as V1Configuration

GRETEL = "gretel"
"""Gretel application name"""

GRETEL_API_KEY = "GRETEL_API_KEY"
"""Env variable to configure Gretel api key."""

GRETEL_ENDPOINT = "GRETEL_ENDPOINT"
"""Env variable name to configure default Gretel endpoint. Defaults
to DEFAULT_GRETEL_ENDPOINT.
"""

GRETEL_CONFIG_FILE = "GRETEL_CONFIG_FILE"
"""Env variable name to override default configuration file location"""

GRETEL_PROJECT = "GRETEL_PROJECT"
"""Env variable name to select default project"""

DEFAULT_GRETEL_ENDPOINT = "https://api.gretel.cloud"
"""Default gretel endpoint"""

GRETEL_PREVIEW_FEATURES = "GRETEL_PREVIEW_FEATURES"
"""Env variable to manage preview features"""

GRETEL_ENVS = [GRETEL_API_KEY, GRETEL_PROJECT]


_custom_logger = None


def get_logger(name: str = None) -> logging.Logger:
    return _custom_logger or logging.getLogger(name)


def configure_custom_logger(logger):
    global _custom_logger
    _custom_logger = logger


log = get_logger(__name__)


class PreviewFeatures(Enum):
    """Manage preview feature configurations"""

    ENABLED = "enabled"
    DISABLED = "disabled"


class GretelClientConfigurationError(Exception):
    ...


T = TypeVar("T", bound=Type)


class RunnerMode(Enum):
    LOCAL = "local"
    CLOUD = "cloud"
    MANUAL = "manual"


DEFAULT_RUNNER = RunnerMode.CLOUD


class ClientConfig:
    """Holds Gretel client configuration details. This can be instantiated from
    a file or environment.
    """

    endpoint: str
    """Gretel API endpoint."""

    api_key: Optional[str] = None
    """Gretel API key."""

    default_project_name: Optional[str] = None
    """Default Gretel project name."""

    default_runner: str = DEFAULT_RUNNER.value
    """Default runner"""

    preview_features: str = PreviewFeatures.DISABLED.value
    """Preview features enabled"""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        api_key: Optional[str] = None,
        default_project_name: Optional[str] = None,
        default_runner: str = DEFAULT_RUNNER.value,
        preview_features: str = PreviewFeatures.DISABLED.value,
    ):
        self.endpoint = (
            endpoint or os.getenv(GRETEL_ENDPOINT) or DEFAULT_GRETEL_ENDPOINT
        )
        self.api_key = api_key or os.getenv(GRETEL_API_KEY)
        self.default_runner = default_runner
        self.default_project_name = (
            default_project_name or os.getenv(GRETEL_PROJECT) or default_project_name
        )
        self.preview_features = os.getenv(GRETEL_PREVIEW_FEATURES) or preview_features

    @classmethod
    def from_file(cls, file_path: Path) -> ClientConfig:
        _check_config_perms(file_path)
        config = json.loads(file_path.read_bytes())
        return cls.from_dict(config)

    @classmethod
    def from_env(cls) -> ClientConfig:
        return cls()

    @classmethod
    def from_dict(cls, source: dict) -> ClientConfig:
        return cls(
            **{k: v for k, v in source.items() if k in cls.__annotations__.keys()}
        )

    def _cert_file(self) -> str:
        ssl_cert_file = os.getenv("SSL_CERT_FILE")
        requests_ca_bundle = os.getenv("REQUESTS_CA_BUNDLE")
        default = certifi.where()

        if ssl_cert_file is not None:
            log.debug(
                f"Overriding default cert file per $SSL_CERT_FILE to {ssl_cert_file}"
            )
            return ssl_cert_file
        elif requests_ca_bundle is not None:
            log.debug(
                f"Overriding default cert file per $REQUESTS_CA_BUNDLE to {requests_ca_bundle}"
            )
            return requests_ca_bundle
        else:
            return default

    def _get_api_client(
        self, max_retry_attempts: int = 3, backoff_factor: float = 1
    ) -> ApiClient:
        # disable log warnings when the retry kicks in
        logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
        if not self.api_key:
            raise GretelClientConfigurationError(
                "Gretel API key was not set. Please check your configuration and try again."
            )
        if not self.api_key.startswith("grt"):
            raise GretelClientConfigurationError(
                "Invalid Gretel API key. Please check your configuration and try again."
            )

        configuration = Configuration(
            host=self.endpoint,
            api_key={"ApiKey": self.api_key},
            ssl_ca_cert=self._cert_file(),
        )
        configuration.retries = Retry(  # type:ignore
            total=max_retry_attempts,
            connect=max_retry_attempts,
            read=max_retry_attempts,
            redirect=max_retry_attempts,
            backoff_factor=backoff_factor,
            status=max_retry_attempts,
            method_whitelist=frozenset(
                {"DELETE", "GET", "HEAD", "OPTIONS", "PUT", "TRACE", "POST"}
            ),
            status_forcelist=frozenset({413, 429, 503, 403}),
            raise_on_status=False,
        )
        return ApiClient(configuration)

    @property
    def email(self) -> str:
        return self.get_api(UsersApi).users_me()["data"]["me"]["email"]

    @property
    def stage(self) -> str:
        if "https://api-dev.gretel.cloud" in self.endpoint:
            return "dev"
        return "prod"

    def get_api(
        self,
        api_interface: Type[T],
        max_retry_attempts: int = 5,
        backoff_factor: float = 1,
    ) -> T:
        """Instantiates and configures an api client for a given
        component interface.

        Args:
            api_interface: The api interface to instantiate
            max_retry_attempts: The number of times to retry a failed
                api request.
            backoff_factor: A back factor to apply between retry
                attempts. A base factor of 2 will applied to this value
                to determine the time between attempts.
        """
        return api_interface(self._get_api_client(max_retry_attempts, backoff_factor))

    def get_v1_api(self, api_interface: Type[T]) -> T:
        api_client = V1ApiClient(
            header_name="Authorization",
            header_value=self.api_key,
            configuration=V1Configuration(host=self.endpoint),
        )

        return api_interface(api_client)

    def _check_project(self, project_name: str = None) -> Optional[str]:
        if not project_name:
            return None
        projects_api = self.get_api(ProjectsApi)
        projects_api.get_project(project_id=project_name)
        return project_name

    def update_default_project(self, project_id: str):
        """Updates the default project.

        Args:
            project_name: The name or id of the project to set.
        """
        self.default_project_name = project_id

    @property
    def as_dict(self) -> dict:
        return {
            prop: getattr(self, prop)
            for prop in self.__annotations__
            if not prop.startswith("_")
        }

    def __eq__(self, other: ClientConfig) -> bool:
        return self.as_dict == other.as_dict

    @property
    def masked(self) -> dict:
        """Returns a masked representation of the config object."""
        c = self.as_dict
        c["api_key"] = self.masked_api_key
        return c

    @property
    def masked_api_key(self) -> str:
        if not self.api_key:
            return "None"

        return self.api_key[:8] + "****"

    @property
    def preview_features_enabled(self) -> bool:
        return self.preview_features == PreviewFeatures.ENABLED.value


_DEFAULT_CONFIG_PATH = Path().home() / f".{GRETEL}" / "config.json"  # noqa


def _get_config_path() -> Path:
    """Returns the path to the system's Gretel config"""
    from_env = os.getenv(GRETEL_CONFIG_FILE)
    if from_env:
        return Path(from_env)
    return _DEFAULT_CONFIG_PATH


def clear_gretel_config():
    """Removes any Gretel configuration files from the host file system.

    If any Gretel related  environment variables exist, this will also remove
    them from the current processes.
    """
    try:
        config = _get_config_path()
        config.unlink()
        config.parent.rmdir()
    except (FileNotFoundError, OSError):
        pass
    for env_var in GRETEL_ENVS:
        if env_var in os.environ:
            del os.environ[env_var]


def _load_config(config_path: Path = None) -> ClientConfig:
    """This will load in a Gretel config that can be used for making
    requests to Gretel's API.

    By default this function will look for a config on the local machine. If that
    config doesn't exist, it will fallback to building a config using environment
    variables on the system.

    Args:
        config_path: Path to a local Gretel config. This defaults to
            ``$HOME/.gretel/config.json``.
    """
    if not config_path:
        config_path = _get_config_path()
    if not config_path.exists():
        return ClientConfig.from_env()
    try:
        return ClientConfig.from_file(config_path)
    except Exception as ex:
        raise GretelClientConfigurationError(
            f"Could not load config from {config_path}"
        ) from ex


def _check_config_perms(config_path: Path):
    if not config_path.exists():
        return
    mode = config_path.stat().st_mode
    if mode & 0o077 == 0:
        return
    log.warn(f"Config file {config_path} is group- and/or world-readable!")
    if config_path == _DEFAULT_CONFIG_PATH:
        # For the default config file, automatically change the mode.
        log.warn("Setting permissions to be readable by the owner only.")
        os.chmod(config_path, mode & ~0o077)
    else:
        # Only issue a warning, but leave it to the user to fix this.
        log.warn(
            f"Please set the correct permissions by running `chmod 0600 {config_path}` as soon as possible."
        )


def write_config(config: ClientConfig, config_path: Union[str, Path] = None) -> Path:
    """Writes a Gretel client config to disk.

    Args:
        config: The client config to write
        config_path: Path to write the config to. If not path is provided, the
            default ``$HOME/.gretel/config.json`` path is used.
    """
    if not config_path:
        config_path = _get_config_path()
    if isinstance(config_path, str):
        config_path = Path(config_path)
    try:
        if not config_path.exists():
            config_path.parent.mkdir(exist_ok=True, parents=True)
            config_path.touch(mode=0o600)
        _check_config_perms(config_path)
        with open(config_path, "w", opener=_create_mode_opener(0o600)) as f:
            f.write(json.dumps(config.as_dict, indent=4) + "\n")
        return config_path
    except Exception as ex:
        raise GretelClientConfigurationError(
            f"Could write config to {config_path}"
        ) from ex


_session_client_config = _load_config()  # noqa


def get_session_config() -> ClientConfig:
    """Return the session's client config"""
    return _session_client_config


def configure_session(
    config: Optional[Union[str, ClientConfig]] = None,
    *,
    api_key: Optional[str] = None,
    endpoint: Optional[str] = None,
    cache: str = "no",
    validate: bool = False,
    clear: bool = False,
):
    """Updates client config for the session

    Args:
        config: The config to update. This config takes precedence over
            other parameters such as ``api_key`` or ``endpoint``.
        api_key: Configures your Gretel API key. If ``api_key`` is set to
            "prompt" and no Api Key is found on the system, ``getpass``
            will be used to prompt for the key.
        endpoint: Specifies the Gretel API endpoint. This must be a fully
            qualified URL.
        cache: Valid options include "yes" and "no". If cache is "no"
            the session configuration will not be written to disk. If cache is
            "yes", session configuration will be written to disk only if a
            configuration doesn't exist.
        validate: If set to ``True`` this will check that login credentials
            are valid.
        clear: If set to ``True`` any existing Gretel credentials will be
            removed from the host.

    Raises:
        ``GretelClientConfigurationError`` if `validate=True` and credentials
            are invalid.
    """
    if clear:
        clear_gretel_config()

    if not config:
        config = _load_config()

    if isinstance(config, str):
        raise NotImplementedError("Gretel URIs are not supported yet.")

    if api_key == "prompt":
        if config.api_key:
            print("Found cached Gretel credentials")
        else:
            api_key = getpass("Gretel Api Key")

    if api_key and api_key.startswith("grt") or endpoint:
        config = ClientConfig(endpoint=endpoint, api_key=api_key)

    if cache == "yes":
        try:
            ClientConfig.from_file(_get_config_path())
        except Exception:
            print("Caching Gretel config to disk.")
            write_config(config)

    global _session_client_config
    if isinstance(config, ClientConfig):
        _session_client_config = config

    if validate:
        print(f"Using endpoint {config.endpoint}")
        try:
            print(f"Logged in as {config.email} \u2705")
        except Exception as ex:
            raise GretelClientConfigurationError(
                "Failed to validate credentials. Please check your config."
            ) from ex


def _create_mode_opener(mode):
    """Returns an opener to be used with open() that sets the file mode."""
    return lambda path, flags: os.open(path, flags, mode=mode)
