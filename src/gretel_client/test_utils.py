from typing import Optional, Type
from unittest.mock import create_autospec, Mock

from requests import Session

from gretel_client.config import ClientConfig, T
from gretel_client.files.interface import FileClient
from gretel_client.navigator_client_protocols import (
    GretelApiProviderProtocol,
    GretelResourceProviderProtocol,
)
from gretel_client.projects.projects import Project
from gretel_client.workflows.manager import WorkflowManager


class TestGretelApiFactory(GretelApiProviderProtocol):

    def __init__(self) -> None:
        self.cached_clients = {}
        self.session = create_autospec(Session)

    def get_api(
        self,
        api_interface: Type[T],
        max_retry_attempts: int = 5,
        backoff_factor: float = 1,
        *,
        default_headers: Optional[dict[str, str]] = None,
    ) -> T:
        if api_interface not in self.cached_clients:
            self.cached_clients[api_interface] = Mock()
        return self.cached_clients.get(api_interface)

    def get_mock(self, api_interface: Type[T]) -> Mock:
        return self.cached_clients.get(api_interface)

    def requests(self) -> Session:
        return self.session

    @property
    def client_config(self) -> ClientConfig:
        return create_autospec(ClientConfig)


class TestGretelResourceProvider(GretelResourceProviderProtocol):

    def __init__(self) -> None:
        self._workflows = create_autospec(WorkflowManager)
        self._files = create_autospec(FileClient)
        self._project = create_autospec(Project)

    @property
    def console_url(self) -> str:
        return "https://console.gretel.cloud"

    @property
    def workflows(self) -> WorkflowManager:
        return self._workflows

    @property
    def project_id(self) -> str:
        return "proj_1"

    @property
    def files(self) -> FileClient:
        return self._files

    @property
    def project(self) -> Project:
        return self._project
