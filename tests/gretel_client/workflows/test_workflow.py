from unittest.mock import Mock

import pytest

from gretel_client.rest_v1.api.workflows_api import WorkflowsApi
from gretel_client.rest_v1.models import WorkflowRun as WorkflowRunApiResponse
from gretel_client.test_utils import TestGretelApiFactory, TestGretelResourceProvider
from gretel_client.workflows.status import Status
from gretel_client.workflows.workflow import WorkflowRun


@pytest.fixture
def workflow_run_response() -> WorkflowRunApiResponse:
    mock = Mock(spec=WorkflowRunApiResponse)
    mock.id = "wr_123"
    mock.workflow_id = "wf_456"
    mock.config = {
        "name": "test-workflow",
        "steps": [
            {
                "name": "generate_data",
                "task": "id_generator",
                "config": {"num_records": 10},
            },
            {
                "name": "evaluate_data",
                "task": "evaluate",
                "config": {"metrics": ["quality"]},
            },
        ],
    }
    mock.status = "abc"
    mock.config_text = "name: test-workflow\nsteps:\n  - name: generate_data\n    task: id_generator\n    config:\n      num_records: 10"
    return mock


def test_initialization_and_factory_method(
    api_provider_mock: TestGretelApiFactory,
    resource_provider_mock: TestGretelResourceProvider,
    workflow_run_response: WorkflowRunApiResponse,
):
    # Test direct initialization
    workflow_run = WorkflowRun(
        workflow_run_response, api_provider_mock, resource_provider_mock
    )

    assert workflow_run.id == "wr_123"
    assert workflow_run.config["name"] == "test-workflow"
    assert len(workflow_run.steps) == 2
    assert workflow_run.steps[0].name == "generate_data"

    # Test factory method
    api_provider_mock.get_api(
        WorkflowsApi
    ).get_workflow_run.return_value = workflow_run_response

    workflow_run_from_factory = WorkflowRun.from_workflow_run_id(
        "wr_123", api_provider_mock, resource_provider_mock
    )

    # Verify the API was called correctly
    api_provider_mock.get_api(WorkflowsApi).get_workflow_run.assert_called_once_with(
        workflow_run_id="wr_123"
    )

    # Verify the factory method creates an equivalent object
    assert workflow_run_from_factory.id == workflow_run.id
    assert workflow_run_from_factory.config == workflow_run.config
    assert workflow_run_from_factory.console_url == workflow_run.console_url


def test_fetch_status(
    api_provider_mock: TestGretelApiFactory,
    resource_provider_mock: TestGretelResourceProvider,
    workflow_run_response: WorkflowRunApiResponse,
):
    for enum_value in Status:
        workflow_run = _prepare_status_mock(
            api_provider_mock,
            resource_provider_mock,
            workflow_run_response,
            enum_value.value,
        )
        status = workflow_run.fetch_status()
        assert enum_value == status

    # Test error case, maybe we want to improve this?
    with pytest.raises(KeyError):
        workflow_run = _prepare_status_mock(
            api_provider_mock, resource_provider_mock, workflow_run_response, "bad val"
        )

        workflow_run.fetch_status()


def _prepare_status_mock(
    api_provider_mock: TestGretelApiFactory,
    resource_provider_mock: TestGretelResourceProvider,
    workflow_run_response: WorkflowRunApiResponse,
    status: str,
) -> WorkflowRun:
    workflow_run_response.status = status
    workflow_run = WorkflowRun(
        workflow_run_response, api_provider_mock, resource_provider_mock
    )

    api_provider_mock.get_api(
        WorkflowsApi
    ).get_workflow_run.return_value = workflow_run_response
    return workflow_run
