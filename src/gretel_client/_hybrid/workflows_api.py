from typing import Any, Dict, Optional

from pydantic.v1 import StrictStr

from gretel_client.rest_v1.api.workflows_api import CreateWorkflowRequest, WorkflowsApi
from gretel_client.rest_v1.api_response import ApiResponse
from gretel_client.rest_v1.models.workflow import Workflow
from gretel_client.workflows.runner_mode import RunnerMode


class HybridWorkflowsApi(WorkflowsApi):
    """
    Hybrid wrapper for the workflows api.

    Objects of this class behave like the regular workflows API,
    with the following exceptions:
    - all created workflows have an implicit "hybrid" runner mode.
      If the creation request explicitly specifies a runner mode,
      this must be "hybrid", otherwise the creation will fail.
    - all workflow validation requests have an implicit "hybrid"
      runner mode. If the creation request explicitly specifies
      a runner mode, this must be "hybrid", otherwise the creation
      will fail.
    """

    def __init__(self, api: WorkflowsApi):
        super().__init__(api.api_client)

    def create_workflow(
        self,
        create_workflow_request: CreateWorkflowRequest,
        **kwargs,
    ) -> Workflow:
        create_workflow_request = _update_runner_mode(create_workflow_request)
        return super().create_workflow(create_workflow_request, **kwargs)

    def create_workflow_with_http_info(
        self, create_workflow_request: CreateWorkflowRequest, **kwargs
    ) -> ApiResponse:
        create_workflow_request = _update_runner_mode(create_workflow_request)
        return super().create_workflow_with_http_info(create_workflow_request, **kwargs)

    def validate_workflow_action_with_http_info(
        self,
        body: Dict[str, Any],
        runner_mode: Optional[StrictStr] = None,
        **kwargs,
    ) -> ApiResponse:
        if runner_mode is None:
            runner_mode = RunnerMode.RUNNER_MODE_HYBRID.value
        elif runner_mode != RunnerMode.RUNNER_MODE_HYBRID.value:
            raise ValueError(
                "in hybrid mode, only runner mode RUNNER_MODE_HYBRID is permissible for workflow action validation"
            )
        return super().validate_workflow_action_with_http_info(
            body, runner_mode, **kwargs
        )


def _update_runner_mode(
    create_workflow_request: CreateWorkflowRequest,
) -> CreateWorkflowRequest:
    if create_workflow_request.runner_mode is None:
        create_workflow_request.runner_mode = RunnerMode.RUNNER_MODE_HYBRID.value
    elif create_workflow_request.runner_mode != RunnerMode.RUNNER_MODE_HYBRID.value:
        raise ValueError(
            "in hybrid mode, only workflows with runner mode RUNNER_MODE_HYBRID can be created"
        )
    return create_workflow_request
