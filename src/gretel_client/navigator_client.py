from __future__ import annotations

import logging
import sys

from contextlib import contextmanager
from enum import Enum
from getpass import getpass
from typing import Iterator, Optional, Type
from urllib.parse import urljoin

import requests

from gretel_client._api.api_client import ApiClient as V2ApiClient
from gretel_client._api.configuration import Configuration as V2Configuration
from gretel_client.config import (
    ClientConfig,
    DefaultClientConfig,
    GretelClientConfigurationError,
    T,
    _load_config,
)
from gretel_client.data_designer import DataDesignerFactory
from gretel_client.files.interface import FileClient
from gretel_client.projects.exceptions import GretelProjectError
from gretel_client.projects.projects import (
    Project,
    create_or_get_unique_project,
    get_project,
    tmp_project,
)
from gretel_client.safe_synthetics.dataset import SafeSyntheticDatasetFactory
from gretel_client.workflows.configs.registry import Registry
from gretel_client.workflows.manager import WorkflowManager
from gretel_client.workflows.tasks import TaskRegistry

logger = logging.getLogger(__name__)


def configure_session(
    *,
    api_key: Optional[str] = None,
    endpoint: Optional[str] = None,
    validate: bool = True,
):
    """Updates client config for the session

    Args:
        api_key: Configures your Gretel API key. If ``api_key`` is set to
            "prompt" and no Api Key is found on the system, ``getpass``
            will be used to prompt for the key.
        endpoint: Specifies the Gretel API endpoint. This must be a fully
            qualified URL.
        validate: If set to ``True`` this will check that login credentials
            are valid.

    Raises:
        ``GretelClientConfigurationError`` if `validate=True` and credentials
            are invalid.
    """

    config = _load_config()

    api_key = api_key or config.api_key
    if api_key == "prompt":
        if config.api_key:
            api_key = config.api_key
            print("Found cached Gretel credentials")
        else:
            api_key = getpass("Gretel API Key: ")

    config = DefaultClientConfig(
        endpoint=endpoint or config.endpoint,
        api_key=api_key or config.api_key,
        default_project_name=config.default_project_name,
    )

    if validate:
        try:
            print(f"Logged in as {config.email} \u2705")
        except Exception as ex:
            raise GretelClientConfigurationError(
                "Failed to validate credentials. Please check your config."
            ) from ex

    return config


class LogConfiguration(Enum):
    """
    Configure Gretel client logging.
    """

    DISABLED = "disabled"
    """
    Don't configure any log handlers. In this configuration, the caller
    must configure the standard python logging interface. This configuration
    is preferable when running the Gretel client from inside a service or
    larger program.
    """

    STANDARD = "standard"
    """
    Configure standard logging handlers that ensure info+ log messages
    are displayed when using the Gretel client. This configuration is
    suited for running inside of notebooks or from python scripts.
    """


class GretelApiProvider:
    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        endpoint: Optional[str] = None,
    ):
        self._client_config = configure_session(api_key=api_key, endpoint=endpoint)

    @property
    def client_config(self) -> ClientConfig:
        return self._client_config

    def get_api(
        self,
        api_interface: Type[T],
        max_retry_attempts: int = 5,
        backoff_factor: float = 1,
        *,
        default_headers: Optional[dict[str, str]] = None,
    ) -> T:
        # we have three (!!) different api client interfaces. until we can
        # converge on one, we'll want to ensure we instantiate the correct
        # client based on the module the `api_interface` belongs to.
        module = api_interface.__module__

        if module.startswith("gretel_client.rest."):
            return self.client_config.get_api(
                api_interface,
                max_retry_attempts,
                backoff_factor,
                default_headers=default_headers,
            )

        if module.startswith("gretel_client.rest_v1."):
            return self.client_config.get_v1_api(
                api_interface,
                max_retry_attempts,
                backoff_factor,
                default_headers=default_headers,
            )

        if module.startswith("gretel_client._api."):
            api_client = self.client_config._get_api_client_generic(
                V2ApiClient,
                V2Configuration,
                max_retry_attempts=max_retry_attempts,
                backoff_factor=backoff_factor,
                default_headers=default_headers,
            )

            return api_interface(api_client)

        raise Exception("Could not get api client for interface")

    def requests(self) -> requests.Session:
        class SessionWithHostname(requests.Session):
            """
            A requests.Session instance cannot configure a common
            host. SessionWithHostname provides a Session that can
            specify a common host.

            https://github.com/psf/requests/issues/2554#issuecomment-109341010
            """

            def __init__(self, prefix_url):
                self.prefix_url = prefix_url
                super(SessionWithHostname, self).__init__()

            def request(self, method, url, *args, **kwargs):
                url = urljoin(self.prefix_url, url)
                return super(SessionWithHostname, self).request(
                    method, url, *args, **kwargs
                )

        if not self.client_config.api_key:
            raise Exception("No api key set")

        session = SessionWithHostname(self.client_config.endpoint)
        session.headers.update({"Authorization": self.client_config.api_key})

        return session


class Gretel:
    """
    The ``Gretel`` class allows you to interact with Gretel Services with an
    easy to use python SDK.

    Args:
        api_key: Configures your Gretel API key. If ``api_key`` is set to
            "prompt" and no Api Key is found on the system, ``getpass``
            will be used to prompt for the key.
        endpoint: Specifies the Gretel API endpoint. This must be a fully
            qualified URL.
        default_project_id: Optional project ID to use. If not provided,
            a default project is created. If a default_project_id is provided
            that does not exist, we will try and create that project.
        create_project: If set to True, create a project from default_project_id
            if one doesn't exist.
        log_configuration: Logging configuration to use. Defaults to STANDARD.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        endpoint: str | None = None,
        default_project_id: str | None = None,
        create_project: bool = True,
        log_configuration: LogConfiguration = LogConfiguration.STANDARD,
        _api_factory: GretelApiProvider | None = None,
    ):
        # cache params so we can create new clients from the current one
        self._init_params = {
            "api_key": api_key,
            "endpoint": endpoint,
            "default_project_id": default_project_id,
            "create_project": create_project,
            "log_configuration": log_configuration,
            "_api_factory": _api_factory,
        }

        self._configure_logger(log_configuration)
        self._api_factory = (
            _api_factory
            if _api_factory
            else GretelApiProvider(api_key=api_key, endpoint=endpoint)
        )

        # setup a project for the client. this project
        # will get used for the entire lifecycle of the
        # Gretel session.
        self._project = self._configure_project(
            default_project_id or self._api_factory.client_config.default_project_name,
            create_project,
        )
        logger.info(f"Using project: {self.project_name}")
        logger.info(f"Project link: {self._project.get_console_url()}")

        # public modules
        self._files = FileClient(
            self._api_factory.client_config, self._api_factory.client_config.endpoint
        )
        self._workflows = WorkflowManager(self._api_factory, self)
        self._tasks = TaskRegistry.create()
        self._safe_synthetic_dataset_factory = SafeSyntheticDatasetFactory(self)
        self._data_designer_factory = DataDesignerFactory(self._api_factory, self)

    def new_client(self, *, default_project_id: str | None = None) -> Gretel:
        init_params = self._init_params
        if default_project_id:
            init_params["default_project_id"] = default_project_id
        return Gretel(**init_params)

    @contextmanager
    def tmp_project(self) -> Iterator[Gretel]:
        """
        Creates a temporary project exposed as a contextmanager. Once
        the contextmanager is closed, the project is automatically
        deleted along with any associated resources bound to that
        project.
        """
        with tmp_project() as proj:
            yield self.new_client(default_project_id=proj.project_guid)

    def _configure_logger(self, log_configuration: LogConfiguration) -> None:
        if log_configuration == LogConfiguration.STANDARD:
            handler = logging.StreamHandler(sys.stdout)
            logging.basicConfig(
                level=logging.INFO,
                format="%(message)s",
                handlers=[handler],
            )

            gretel_logger = logging.getLogger("gretel_client")
            gretel_logger.setLevel(logging.INFO)

    def _configure_project(
        self, project_id: str | None = None, create_project: bool = False
    ) -> Project:
        # No project is configured, try and create a default project for the SDK.
        if not project_id:
            if not create_project:
                raise ValueError(
                    "create_project is false, and no project_id was passed. "
                    "Please configure a project by passing default_project_id."
                )

            project = create_or_get_unique_project(
                name="default-sdk-project", session=self._api_factory.client_config
            )

            if not project.project_guid:
                raise Exception("Could not get guid for project")

            return project

        # We have a project id. Try and either load it, or create one.
        if project_id:
            # If a project_id is passed, try and load in the project.
            if project_id.startswith("proj_"):
                return get_project(name=project_id)

            # Otherwise try and load the project by name.
            if create_project:
                # It's possible we receive a project_name produced by
                # create_or_get_unique_project, which means it already
                # contains a unique key, eg my-project-92fbf5bd66089f1. If
                # that's the case, we should first try to look up that project.
                try:
                    return get_project(
                        name=project_id, session=self._api_factory.client_config
                    )
                except GretelProjectError:
                    pass

                # If it doesn't exist, we can create that project using
                # the uniqueness logic from create_or_get_unique_project.
                return create_or_get_unique_project(
                    name=project_id, session=self._api_factory.client_config
                )

            # If create project isn't setup, simply try to lookup the project
            # by it's key.
            return get_project(
                name=project_id,
                session=self._api_factory.client_config,
            )

        # TODO: link to gretel entrypoint docs in the exception once they
        # are published.
        raise GretelProjectError("Could not configure a gretel project")

    @property
    def project_id(self) -> str:
        """The project_id associated with the current session."""
        return self.project.project_guid

    @property
    def project_name(self) -> str:
        return self.project.name

    @property
    def workflows(self) -> WorkflowManager:
        """Provides SDK access to Gretel Workflows."""
        return self._workflows

    @property
    def tasks(self) -> Registry:
        return self._tasks

    @property
    def files(self) -> FileClient:
        return self._files

    @property
    def project(self) -> Project:
        return self._project

    @property
    def safe_synthetic_dataset(self) -> SafeSyntheticDatasetFactory:
        return self._safe_synthetic_dataset_factory

    @property
    def data_designer(self) -> DataDesignerFactory:
        """Create a Data Designer session."""
        return self._data_designer_factory

    @property
    def console_url(self) -> str:
        """Return a URL for accessing the gretel console."""
        return f"https://console{'-dev' if 'dev' in self._api_factory.client_config.endpoint else ''}.gretel.ai"
