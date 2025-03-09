import logging
import sys

from enum import StrEnum
from getpass import getpass
from typing import Optional, Type
from urllib.parse import urljoin

import requests

from gretel_client._api.api_client import ApiClient as V2ApiClient
from gretel_client._api.configuration import Configuration as V2Configuration
from gretel_client.config import (
    _load_config,
    ClientConfig,
    DefaultClientConfig,
    GretelClientConfigurationError,
    T,
)
from gretel_client.files.interface import FileClient
from gretel_client.projects.projects import (
    create_or_get_unique_project,
    get_project,
    Project,
)
from gretel_client.rest.api.projects_api import ProjectsApi
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


class LogConfiguration(StrEnum):
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


class ApiFactory:
    # todo: rename me to ApiProvider??

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

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        endpoint: Optional[str] = None,
        default_project_id: Optional[str] = None,
        log_configuration: LogConfiguration = LogConfiguration.STANDARD,
        _api_factory: Optional[ApiFactory] = None,
    ):
        self.configure_logger(log_configuration)
        self._api_factory = (
            _api_factory
            if _api_factory
            else ApiFactory(api_key=api_key, endpoint=endpoint)
        )

        # setup a project for the client. this project
        # will get used for the entire lifecycle of the
        # Gretel session.
        self._project_id = self._configure_project(
            default_project_id or self._api_factory.client_config.default_project_name
        )
        logger.info(f"Gretel client configured to use project: {self.project_id}")

        # public modules
        self._files = FileClient(
            self._api_factory.client_config, self._api_factory.client_config.endpoint
        )
        self._workflows = WorkflowManager(self._api_factory, self)
        self._tasks = TaskRegistry.create()
        self._safe_synthetic_data_factory = SafeSyntheticDatasetFactory(self)

    def configure_logger(self, log_configuration: LogConfiguration):
        if log_configuration == LogConfiguration.STANDARD:
            handler = logging.StreamHandler(sys.stdout)
            logging.basicConfig(
                level=logging.INFO,
                format="%(message)s",
                handlers=[handler],
            )

            gretel_logger = logging.getLogger("gretel_client")
            gretel_logger.setLevel(logging.INFO)

    def _configure_project(self, project_id: Optional[str] = None) -> str:
        if not project_id:
            project = create_or_get_unique_project(
                name="default-sdk-project", session=self._api_factory.client_config
            )

            if not project.project_guid:
                raise Exception("Could not get guid for project")

            return project.project_guid

        if project_id.startswith("proj_"):
            return project_id

        elif project_id:
            projects_api = self._api_factory.get_api(ProjectsApi)
            resp = projects_api.get_project(project_id=project_id)
            return resp["data"]["project"]["guid"]

        raise Exception("Could not configure a gretel project")

    @property
    def project_id(self) -> str:
        return self._project_id

    @property
    def workflows(self) -> WorkflowManager:
        return self._workflows

    @property
    def tasks(self) -> Registry:
        return self._tasks

    @property
    def files(self) -> FileClient:
        return self._files

    @property
    def project(self) -> Project:
        if not self._project:
            self._project = get_project(name=self._project_id)
        return self._project

    @property
    def safe_synthetic_dataset(self) -> SafeSyntheticDatasetFactory:
        return self._safe_synthetic_data_factory

    @property
    def console_url(self) -> str:
        return f"https://console{'-dev' if 'dev' in self._api_factory.client_config.endpoint else ''}.gretel.ai"
