from typing import Any

from gretel_client._api.api.workflows_api import WorkflowsApi as V2WorkflowsApi
from gretel_client._api.exceptions import NotFoundException
from gretel_client._api.models.llm_suite_config_with_generation_params import (
    LLMSuiteConfigWithGenerationParams,
)
from gretel_client._api.models.validate_model_suite_request import (
    ValidateModelSuiteRequest,
)
from gretel_client.navigator_client_protocols import (
    GretelApiProviderProtocol,
    GretelResourceProviderProtocol,
)
from gretel_client.rest_v1.api.workflows_api import WorkflowsApi
from gretel_client.workflows.builder import WorkflowBuilder, WorkflowSessionManager
from gretel_client.workflows.configs.workflows import Globals, ModelConfig
from gretel_client.workflows.tasks import task_to_step, TaskConfig
from gretel_client.workflows.workflow import WorkflowRun


class WorkflowManager:
    """
    Provides a low-level interface for interacting with Gretel Workflows.

    Note: This class should never be directly instantiated. Instead you
    should interact with the class from the Gretel client session.

    For example

        To fetch an existing Workflow Run::

            from gretel.navigator_client import Gretel

            gretel = Gretel()
            workflow = gretel.workflows.get_workflow_run("wr_run_id_here")
    """

    def __init__(
        self,
        api_factory: GretelApiProviderProtocol,
        resource_provider: GretelResourceProviderProtocol,
    ) -> None:
        self._api_provider = api_factory
        self._workflow_api = api_factory.get_api(WorkflowsApi)
        self._data_api = api_factory.get_api(V2WorkflowsApi)
        self._resource_provider = resource_provider

        self._workflow_session_manager = WorkflowSessionManager()

    def builder(
        self,
        globals: Globals | None = None,
    ) -> WorkflowBuilder:
        """
        Creates a new workflow builder instance. This can be used to construct
        Workflows using a fluent builder pattern.

        Args:
            globals: Configure global variables for the Workflow.

        Returns:
            WorkflowBuilder: A fluent builder to construct Workflows.
        """
        return WorkflowBuilder(
            self._resource_provider.project_id,
            globals or Globals(),
            self._api_provider,
            self._resource_provider,
            self._workflow_session_manager,
        )

    def create(
        self, tasks: list[TaskConfig], wait_until_done: bool = False
    ) -> WorkflowRun:
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
        return builder.run(wait_until_done=wait_until_done)

    def registry(self) -> dict[str, Any]:
        """
        Retrieves the workflow registry.

        Returns:
            object: The workflow registry.
        """
        # todo: create a registry type here
        return self._data_api.get_workflow_registry()

    def get_workflow_run(self, workflow_run_id: str) -> WorkflowRun:
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

    def get_model_suites(self) -> list[LLMSuiteConfigWithGenerationParams]:
        return self._data_api.get_model_suites().model_suites

    def validate_model_suite(
        self, model_suite: str, model_configs: list[ModelConfig]
    ) -> list[str]:
        ## TODO: remove this try catch on NotFoundException once the prod API is updated
        try:
            return self._data_api.validate_model_suite(
                model_suite=model_suite,
                validate_model_suite_request=ValidateModelSuiteRequest(
                    model_configs=[mc.model_dump() for mc in model_configs]
                ),
            ).violations
        except NotFoundException:
            return []
