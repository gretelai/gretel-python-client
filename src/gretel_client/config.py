from __future__ import annotations

import json
import logging
import os
import platform

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from enum import Enum
from functools import cached_property
from getpass import getpass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Optional, Type, TypeVar, Union
from urllib.parse import quote_plus

import certifi

from urllib3 import HTTPResponse
from urllib3.exceptions import MaxRetryError
from urllib3.util import Retry

from gretel_client.rest.api.projects_api import ProjectsApi
from gretel_client.rest.api.users_api import UsersApi
from gretel_client.rest.api_client import ApiClient
from gretel_client.rest.configuration import Configuration
from gretel_client.rest_v1.api.serverless_api import ServerlessApi
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

GRETEL_ARTIFACT_ENDPOINT = "GRETEL_ARTIFACT_ENDPOINT"
"""Env variable name to configure artifact endpoint. Defaults
to DEFAULT_GRETEL_ARTIFACT_ENDPOINT.
"""

GRETEL_CONFIG_FILE = "GRETEL_CONFIG_FILE"
"""Env variable name to override default configuration file location"""

GRETEL_PROJECT = "GRETEL_PROJECT"
"""Env variable name to select default project"""

GRETEL_RUNNER_MODE = "GRETEL_RUNNER_MODE"
"""Env variable name to set the default runner mode"""

DEFAULT_GRETEL_ENDPOINT = "https://api.gretel.cloud"
"""Default gretel endpoint"""

DEFAULT_GRETEL_ARTIFACT_ENDPOINT = "cloud"
"""Default artifact endpoint"""

GRETEL_PREVIEW_FEATURES = "GRETEL_PREVIEW_FEATURES"
"""Env variable to manage preview features"""

GRETEL_TENANT_NAME = "GRETEL_TENANT_NAME"
"""Enterprise tenant name"""

GRETEL_TENANT_UNSET = "none"
"""Value to indicate that a tenant name should be unset"""

GRETEL_ENVS = [GRETEL_API_KEY, GRETEL_PROJECT, GRETEL_TENANT_NAME]

CLIENT_METRICS_HEADER_KEY = "X-Gretel-Client-Metrics"


_custom_logger = None


def get_logger(name: str = None) -> logging.Logger:
    return _custom_logger or logging.getLogger(name)


def configure_custom_logger(logger):
    global _custom_logger
    _custom_logger = logger


log = get_logger(__name__)


@dataclass
class Context:
    client_metrics: dict[str, str]
    job_provenance: dict[str, str]

    @classmethod
    def empty(cls) -> Context:
        return Context(client_metrics={}, job_provenance={})

    def update(
        self, client_metrics: dict[str, str], job_provenance: dict[str, str]
    ) -> Context:
        return replace(
            self,
            client_metrics=self.client_metrics | client_metrics,
            job_provenance=self.job_provenance | job_provenance,
        )


def _get_client_version() -> str:
    try:
        return version("gretel_client")
    except PackageNotFoundError:
        return "undefined"


def _metrics_headers(context: Optional[Context] = None) -> dict[str, str]:
    metadata = {"python_sdk_version": _get_client_version()}
    if context:
        metadata |= context.client_metrics

    stringified = ";".join((f"{k}={v}" for k, v in metadata.items()))

    return {CLIENT_METRICS_HEADER_KEY: quote_plus(stringified)}


class PreviewFeatures(Enum):
    """Manage preview feature configurations"""

    ENABLED = "enabled"
    DISABLED = "disabled"


class GretelClientConfigurationError(Exception): ...


T = TypeVar("T", bound=Type)
ClientT = TypeVar("ClientT", bound=Union[ApiClient, V1ApiClient])
ConfigT = TypeVar("ConfigT", bound=Union[Configuration, V1Configuration])


class RunnerMode(str, Enum):
    LOCAL = "local"
    CLOUD = "cloud"
    MANUAL = "manual"
    HYBRID = "hybrid"

    @classmethod
    def parse(cls, runner_mode: Union[str, RunnerMode]) -> RunnerMode:
        if not isinstance(runner_mode, RunnerMode):
            try:
                runner_mode = RunnerMode(runner_mode)
            except ValueError:
                raise ValueError(f"Invalid runner_mode: {runner_mode}")
        return runner_mode

    @classmethod
    def parse_optional(
        cls, runner_mode: Optional[Union[str, RunnerMode]]
    ) -> Optional[RunnerMode]:
        if not runner_mode:
            return None

        return cls.parse(runner_mode)

    def __str__(self) -> str:
        return self.value

    @property
    def api_mode(self) -> RunnerMode:
        if self == RunnerMode.LOCAL:
            return RunnerMode.MANUAL
        return self

    @property
    def api_value(self) -> str:
        return self.api_mode.value


DEFAULT_RUNNER = RunnerMode.CLOUD


class GretelApiRetry(Retry):
    """
    Custom retry logic for calling Gretel Cloud APIs.
    """

    # Message, which is returned in the response body for throttled requests.
    _THROTTLE_403_TEXT = "User is not authorized to access this resource"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    @classmethod
    def create_default(
        cls, *, max_retry_attempts: int, backoff_factor: float
    ) -> GretelApiRetry:
        return cls(
            total=max_retry_attempts,
            connect=max_retry_attempts,
            read=max_retry_attempts,
            redirect=max_retry_attempts,
            backoff_factor=backoff_factor,
            status=max_retry_attempts,
            allowed_methods=frozenset(
                {"DELETE", "GET", "HEAD", "OPTIONS", "PUT", "TRACE", "POST"}
            ),
            status_forcelist=frozenset({413, 429, 502, 503, 504, 403}),
            raise_on_status=False,
        )

    def increment(
        self,
        method=None,
        url=None,
        response=None,
        error=None,
        _pool=None,
        _stacktrace=None,
    ):
        if self._was_forbidden(response):
            # no need to retry
            raise MaxRetryError(_pool, url, error)

        return super().increment(method, url, response, error, _pool, _stacktrace)

    def _was_forbidden(self, response: HTTPResponse) -> bool:
        if not response or response.status != 403:
            return False

        # With how our APIs are configured, 403 may mean that:
        #  - call is not authorized (no need to retry)
        #  - call is rate limited (retry)
        # To differentiate between these 2 we check for a specific text in the body.
        was_rate_limited = GretelApiRetry._THROTTLE_403_TEXT in str(response.data)
        return not was_rate_limited


class ClientConfig(ABC):
    def __new__(cls, *args, **kwargs):
        if cls == ClientConfig:
            _cls = DefaultClientConfig
        else:
            _cls = cls
        return super().__new__(_cls)

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
            **{
                k: v
                for k, v in source.items()
                if k in DefaultClientConfig.__annotations__.keys()
            }
        )

    @property
    @abstractmethod
    def context(self) -> Context: ...

    @property
    @abstractmethod
    def endpoint(self) -> str: ...

    @property
    @abstractmethod
    def artifact_endpoint(self) -> str: ...

    @property
    @abstractmethod
    def api_key(self) -> Optional[str]: ...

    @property
    @abstractmethod
    def default_project_name(self) -> Optional[str]: ...

    @property
    @abstractmethod
    def default_runner(self) -> Optional[str]: ...

    @property
    @abstractmethod
    def preview_features(self) -> str: ...

    @property
    @abstractmethod
    def tenant_name(self) -> Optional[str]: ...

    @cached_property
    def email(self) -> str:
        return self.get_api(UsersApi).users_me()["data"]["me"]["email"]

    @property
    def stage(self) -> str:
        if (
            "https://api-dev.gretel" in self.endpoint
            or ".dev.gretel.cloud" in self.endpoint
        ):
            return "dev"
        return "prod"

    @abstractmethod
    def _get_api_client_generic(
        self,
        client_cls: Type[ClientT],
        config_cls: Type[ConfigT],
        max_retry_attempts: int = 3,
        backoff_factor: float = 1,
        verify_ssl: bool = True,
        *,
        default_headers: Optional[dict[str, str]] = None,
    ) -> ClientT: ...

    def _get_api_client(self, *args, **kwargs) -> ApiClient:
        return self._get_api_client_generic(ApiClient, Configuration, *args, **kwargs)

    def _get_v1_api_client(self, *args, **kwargs) -> V1ApiClient:
        return self._get_api_client_generic(
            V1ApiClient, V1Configuration, *args, **kwargs
        )

    def set_serverless_api(self) -> bool:
        serverless: ServerlessApi = self.get_v1_api(ServerlessApi)
        serverless_tenants_resp = serverless.list_serverless_tenants()
        if (
            isinstance(serverless_tenants_resp.tenants, list)
            and len(serverless_tenants_resp.tenants) > 0
        ):
            for tenant in serverless_tenants_resp.tenants:
                if tenant.name != self.tenant_name:
                    continue
                tenant_endpoint = tenant.config.api_endpoint
                if not tenant_endpoint.startswith("https://"):
                    tenant_endpoint = f"https://{tenant_endpoint}"
                if tenant_endpoint != self.endpoint:
                    print(
                        "Found a serverless tenant associated with this API key. Updating client configuration to use the tenant API endpoint."
                    )
                    self.endpoint = tenant_endpoint
                return True
        return False

    def get_api(
        self,
        api_interface: Type[T],
        max_retry_attempts: int = 5,
        backoff_factor: float = 1,
        *,
        default_headers: Optional[dict[str, str]] = None,
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
        return api_interface(
            self._get_api_client(
                max_retry_attempts, backoff_factor, default_headers=default_headers
            )
        )

    def get_v1_api(
        self,
        api_interface: Type[T],
        max_retry_attempts: int = 5,
        backoff_factor: float = 1,
        *,
        default_headers: Optional[dict[str, str]] = None,
    ) -> T:
        return api_interface(
            self._get_v1_api_client(
                max_retry_attempts, backoff_factor, default_headers=default_headers
            )
        )

    @abstractmethod
    def update_default_project(self, project_id: str): ...

    @property
    @abstractmethod
    def as_dict(self) -> dict: ...

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


class DefaultClientConfig(ClientConfig):
    """Holds Gretel client configuration details. This can be instantiated from
    a file or environment.
    """

    endpoint: str = ""
    """Gretel API endpoint."""

    artifact_endpoint: str = ""
    """Artifact endpoint."""

    api_key: Optional[str] = None
    """Gretel API key."""

    default_project_name: Optional[str] = None
    """Default Gretel project name."""

    default_runner: str = DEFAULT_RUNNER.value
    """Default runner"""

    preview_features: str = PreviewFeatures.DISABLED.value
    """Preview features enabled"""

    tenant_name: Optional[str] = None
    """Enterprise tenant name"""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        artifact_endpoint: Optional[str] = None,
        api_key: Optional[str] = None,
        default_project_name: Optional[str] = None,
        default_runner: Optional[Union[str, RunnerMode]] = None,
        preview_features: Optional[str] = None,
        tenant_name: Optional[str] = None,
    ):
        self.endpoint = (
            endpoint or os.getenv(GRETEL_ENDPOINT) or DEFAULT_GRETEL_ENDPOINT
        )
        self.artifact_endpoint = (
            artifact_endpoint
            or os.getenv(GRETEL_ARTIFACT_ENDPOINT)
            or DEFAULT_GRETEL_ARTIFACT_ENDPOINT
        ).removesuffix("/")
        self.api_key = api_key or os.getenv(GRETEL_API_KEY)
        self.default_runner = RunnerMode.parse(
            default_runner or os.getenv(GRETEL_RUNNER_MODE) or DEFAULT_RUNNER
        )
        self.default_project_name = default_project_name or os.getenv(GRETEL_PROJECT)
        self.preview_features = (
            preview_features
            or os.getenv(GRETEL_PREVIEW_FEATURES)
            or PreviewFeatures.DISABLED.value
        )
        self.tenant_name = tenant_name or os.getenv(GRETEL_TENANT_NAME)
        self._validate()

    def _validate(self) -> None:
        if (
            self.artifact_endpoint != DEFAULT_GRETEL_ARTIFACT_ENDPOINT
            and self.default_runner == RunnerMode.CLOUD
        ):
            raise GretelClientConfigurationError(
                "Cannot use a custom artifact endpoint with cloud runner mode as default. Please reconfigure your session."
            )

        if (
            self.artifact_endpoint == DEFAULT_GRETEL_ARTIFACT_ENDPOINT
            and self.default_runner == RunnerMode.HYBRID
        ):
            raise GretelClientConfigurationError(
                "Hybrid runner requires a custom artifact endpoint. Please reconfigure your session."
            )

    @property
    def context(self) -> Context:
        return Context.empty()

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

    def _determine_proxy(self) -> Optional[str]:
        if "all_proxy" in os.environ:
            return os.environ.get("all_proxy")
        if (
            self.endpoint
            and self.endpoint.startswith("https")
            and "https_proxy" in os.environ
        ):
            return os.environ.get("https_proxy")
        return os.environ.get("http_proxy")

    def _get_api_client_generic(
        self,
        client_cls: Type[ClientT],
        config_cls: Type[ConfigT],
        max_retry_attempts: int = 3,
        backoff_factor: float = 1,
        verify_ssl: bool = True,
        *,
        default_headers: Optional[dict[str, str]] = None,
    ) -> ClientT:
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

        configuration = config_cls(
            host=self.endpoint,
            api_key={"ApiKey": self.api_key},
            ssl_ca_cert=self._cert_file(),
        )
        client_kwargs = {}
        if "ApiKey" not in configuration.auth_settings():
            # ApiKey not recognized as authentication method by config,
            # need to pass header directly.
            client_kwargs = {
                "header_name": "Authorization",
                "header_value": self.api_key,
            }
        configuration.proxy = self._determine_proxy()
        configuration.retries = GretelApiRetry.create_default(  # type:ignore
            max_retry_attempts=max_retry_attempts,
            backoff_factor=backoff_factor,
        )
        configuration.verify_ssl = verify_ssl
        client = client_cls(configuration, **client_kwargs)
        client.default_headers.update(_metrics_headers() | (default_headers or {}))
        return client

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


class DelegatingClientConfig(ClientConfig):
    _delegate: ClientConfig

    def __init__(self, delegate: ClientConfig):
        self._delegate = delegate

    @property
    def context(self) -> Context:
        return self._delegate.context

    @property
    def endpoint(self) -> str:
        return self._delegate.endpoint

    @property
    def artifact_endpoint(self) -> str:
        return self._delegate.artifact_endpoint

    @property
    def api_key(self) -> Optional[str]:
        return self._delegate.api_key

    @property
    def default_project_name(self) -> Optional[str]:
        return self._delegate.default_project_name

    @property
    def default_runner(self) -> Optional[str]:
        return self._delegate.default_runner

    @property
    def preview_features(self) -> str:
        return self._delegate.preview_features

    @property
    def tenant_name(self) -> str:
        return self._delegate.tenant_name

    def _get_api_client_generic(
        self,
        client_cls: Type[ClientT],
        config_cls: Type[ConfigT],
        max_retry_attempts: int = 3,
        backoff_factor: float = 1,
        verify_ssl: bool = True,
        *,
        default_headers: Optional[dict[str, str]] = None,
    ) -> ClientT:
        return self._delegate._get_api_client_generic(
            client_cls=client_cls,
            config_cls=config_cls,
            max_retry_attempts=max_retry_attempts,
            backoff_factor=backoff_factor,
            verify_ssl=verify_ssl,
            default_headers=default_headers,
        )

    def update_default_project(self, project_id: str):
        self._delegate.update_default_project(project_id)

    @property
    def as_dict(self) -> dict:
        return self._delegate.as_dict


class TaggedClientConfig(DelegatingClientConfig):

    _context: Context

    def __init__(self, delegate: ClientConfig, context: Context):
        super().__init__(delegate)
        self._context = context

    @property
    def context(self) -> Context:
        return self._context

    def _get_api_client_generic(
        self,
        client_cls: Type[ClientT],
        config_cls: Type[ConfigT],
        max_retry_attempts: int = 3,
        backoff_factor: float = 1,
        verify_ssl: bool = True,
        *,
        default_headers: Optional[dict[str, str]] = None,
    ) -> ClientT:
        all_headers = _metrics_headers(self.context) | (default_headers or {})
        return super()._get_api_client_generic(
            client_cls=client_cls,
            config_cls=config_cls,
            max_retry_attempts=max_retry_attempts,
            backoff_factor=backoff_factor,
            verify_ssl=verify_ssl,
            default_headers=all_headers,
        )

    def get_api(
        self,
        api_interface: Type[T],
        max_retry_attempts: int = 5,
        backoff_factor: float = 1,
        *,
        default_headers: Optional[dict[str, str]] = None,
    ) -> T:
        all_headers = _metrics_headers(self.context)
        if default_headers:
            all_headers |= default_headers
        return super().get_api(
            api_interface,
            max_retry_attempts,
            backoff_factor,
            default_headers=all_headers,
        )

    def get_v1_api(
        self,
        api_interface: Type[T],
        max_retry_attempts: int = 5,
        backoff_factor: float = 1,
        *,
        default_headers: Optional[dict[str, str]] = None,
    ) -> T:
        all_headers = _metrics_headers(self.context)
        if default_headers:
            all_headers |= default_headers
        return super().get_v1_api(
            api_interface,
            max_retry_attempts,
            backoff_factor,
            default_headers=all_headers,
        )


def add_session_context(
    *,
    session: Optional[ClientConfig] = None,
    client_metrics: Optional[dict[str, str]] = None,
    job_provenance: Optional[dict[str, str]] = None,
) -> TaggedClientConfig:
    delegate = session or get_session_config()

    curr_context = delegate.context or Context.empty()
    new_context = curr_context.update(
        client_metrics=client_metrics or {},
        job_provenance=job_provenance or {},
    )

    return TaggedClientConfig(context=new_context, delegate=delegate)


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
    config_path = config_path or _get_config_path()
    if not config_path.exists():
        return ClientConfig.from_env()

    try:
        return ClientConfig.from_file(config_path)
    except Exception as ex:
        raise GretelClientConfigurationError(
            f"Could not load config from {config_path}"
        ) from ex


def _check_config_perms(config_path: Path):
    if platform.system() == "Windows":
        # Windows permissioning is different
        return
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
    global _session_client_config
    return _session_client_config


def set_session_config(config: ClientConfig):
    global _session_client_config
    _session_client_config = config


def configure_session(
    config: Optional[Union[str, ClientConfig]] = None,
    *,
    api_key: Optional[str] = None,
    default_runner: Optional[Union[str, RunnerMode]] = None,
    endpoint: Optional[str] = None,
    artifact_endpoint: Optional[str] = None,
    cache: str = "no",
    validate: bool = True,
    tenant_name: Optional[str] = None,
    clear: bool = False,
):
    """Updates client config for the session

    Args:
        config: The config to update. This config takes precedence over
            other parameters such as ``api_key`` or ``endpoint``.
        api_key: Configures your Gretel API key. If ``api_key`` is set to
            "prompt" and no Api Key is found on the system, ``getpass``
            will be used to prompt for the key.
        default_runner: Specifies the runner mode. Must be one of "cloud", "local",
            "manual", or "hybrid".
        endpoint: Specifies the Gretel API endpoint. This must be a fully
            qualified URL.
        artifact_endpoint: Specifies the endpoint for project and model artifacts.
        cache: Valid options include "yes" and "no". If cache is "no"
            the session configuration will not be written to disk. If cache is
            "yes", session configuration will be written to disk only if a
            configuration doesn't exist.
        validate: If set to ``True`` this will check that login credentials
            are valid.
        tenant_name: Specifies the Gretel Enterprise tenant name.
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

    default_runner = default_runner or config.default_runner
    default_runner = RunnerMode.parse(default_runner)

    artifact_endpoint = artifact_endpoint or config.artifact_endpoint
    tenant_name = tenant_name or config.tenant_name

    api_key = api_key or config.api_key
    if api_key == "prompt":
        if config.api_key:
            api_key = config.api_key
            print("Found cached Gretel credentials")
        else:
            api_key = getpass("Gretel API Key: ")

    if api_key and api_key.startswith("grt") or endpoint:
        endpoint = endpoint or config.endpoint
        config = ClientConfig(
            endpoint=endpoint,
            artifact_endpoint=artifact_endpoint,
            api_key=api_key,
            default_runner=default_runner,
            default_project_name=config.default_project_name,
            tenant_name=tenant_name,
        )

    if cache == "yes":
        try:
            ClientConfig.from_file(_get_config_path())
        except Exception:
            print("Caching Gretel config to disk.")
            write_config(config)

    global _session_client_config
    if isinstance(config, ClientConfig):
        _session_client_config = config

    if not validate and tenant_name:
        raise GretelClientConfigurationError(
            "Cannot run without validation when specifying a tenant name."
        )

    if tenant_name:
        if config.set_serverless_api():
            print(f"Using endpoint {config.endpoint}")
        else:
            raise GretelClientConfigurationError(
                f"Could not find serverless tenant {tenant_name} associated with this API key."
            )

    if validate:
        try:
            print(f"Logged in as {config.email} \u2705")
        except Exception as ex:
            raise GretelClientConfigurationError(
                "Failed to validate credentials. Please check your config."
            ) from ex


def get_data_plane_endpoint(session: Optional[ClientConfig] = None) -> str:
    if session is None:
        session = get_session_config()

    if any(
        session.endpoint.endswith(token)
        for token in [
            "sandbox.dev.gretel.ai",
            "enterprise.dev.gretel.ai",
            "serverless.dev.gretel.ai",
            "enterprise.gretel.ai",
            "serverless.gretel.ai",
        ]
    ):
        return session.endpoint

    if "api-dev" in session.endpoint:
        return "https://dataplane.dev.gretel.cloud"

    return "https://dataplane.gretel.cloud"


def _create_mode_opener(mode):
    """Returns an opener to be used with open() that sets the file mode."""
    return lambda path, flags: os.open(path, flags, mode=mode)
