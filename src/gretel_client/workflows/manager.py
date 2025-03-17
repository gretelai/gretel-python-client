from typing import Any

from gretel_client._api.api.default_api import DefaultApi
from gretel_client.navigator_client_protocols import (
    GretelApiProviderProtocol,
    GretelResourceProviderProtocol,
)
from gretel_client.rest_v1.api.workflows_api import WorkflowsApi
from gretel_client.workflows.builder import WorkflowBuilder, WorkflowSessionManager
from gretel_client.workflows.configs.workflows import Globals
from gretel_client.workflows.tasks import task_to_step, TaskConfig
from gretel_client.workflows.workflow import WorkflowRun


class WorkflowManager:
    """Provides a low-level interface for interacting with Gretel workflow"""

    def __init__(
        self,
        api_factory: GretelApiProviderProtocol,
        resource_provider: GretelResourceProviderProtocol,
    ) -> None:
        self._api_provider = api_factory
        self._workflow_api = api_factory.get_api(WorkflowsApi)
        self._data_api = api_factory.get_api(DefaultApi)
        self._resource_provider = resource_provider

        self._workflow_session_manager = WorkflowSessionManager()

    def builder(
        self,
        globals: Globals | None = None,
    ) -> WorkflowBuilder:
        """Creates a new workflow builder instance.

        Returns:
            WorkflowBuilder: A builder to construct workflows.
        """
        return WorkflowBuilder(
            self._resource_provider.project_id,
            globals or Globals(),
            self._api_provider,
            self._resource_provider,
            self._workflow_session_manager,
        )

    def create(self, tasks: list[TaskConfig]) -> WorkflowRun:
        """
        Creates and executes a workflow from a list of task configurations.

        Args:
            tasks: List of task configurations to include in the workflow.

        Returns:
            WorkflowRun: The executed workflow run instance.
        """
        builder = self.builder()
        for task in tasks:
            builder.add_step(task_to_step(task))
        return builder.run()

    def registry(self) -> dict[str, Any]:
        """
        Retrieves the workflow registry.

        Returns:
            object: The workflow registry.
        """
        # todo: create a registry type here
        return self._data_api.registry_v2_workflows_registry_get()

    def get_workflow_run(self, workflow_run_id) -> WorkflowRun:
        """
        Retrieves a specific workflow run by ID.

        Args:
            workflow_run_id: The ID of the workflow run to retrieve.

        Returns:
            WorkflowRun: The workflow run instance.
        """
        return WorkflowRun.from_workflow_run_id(
            workflow_run_id, self._api_provider, self._resource_provider
        )
