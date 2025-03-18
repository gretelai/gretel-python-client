import datetime
import json

from unittest.mock import ANY, create_autospec, Mock, patch

import pytest

from gretel_client._api.api.default_api import DefaultApi
from gretel_client._api.exceptions import ApiException
from gretel_client._api.models.exec_batch_request import ExecBatchRequest
from gretel_client._api.models.exec_batch_response import ExecBatchResponse
from gretel_client._api.models.task_envelope import TaskEnvelope
from gretel_client._api.models.workflow_input import WorkflowInput
from gretel_client.test_utils import TestGretelApiFactory, TestGretelResourceProvider
from gretel_client.workflows.builder import (
    _generate_workflow_name,
    WorkflowBuilder,
    WorkflowSessionManager,
    WorkflowValidationError,
)
from gretel_client.workflows.configs.registry import Registry
from gretel_client.workflows.configs.workflows import Globals, Step, Workflow


@pytest.fixture
def tasks() -> Registry:
    return Registry()


@pytest.fixture
def stub_globals() -> Globals:
    return Globals()


@pytest.fixture
def builder(
    api_provider_mock: TestGretelApiFactory,
    resource_provider_mock: TestGretelResourceProvider,
    stub_globals: Globals,
) -> WorkflowBuilder:
    return WorkflowBuilder(
        "proj_1", stub_globals, api_provider_mock, resource_provider_mock
    )


def test_workflow_default_name(builder: WorkflowBuilder):
    assert builder.to_workflow().name.startswith("my-workflow")


def test_workflow_custom_name(builder: WorkflowBuilder):
    builder.set_name("my-name")
    assert builder.to_workflow().name == "my-name"


def test_workflow_builder_add_step_from_task(builder: WorkflowBuilder, tasks: Registry):
    builder.add_step(tasks.IdGenerator(num_records=5))

    actual = builder.to_workflow()
    expected = Workflow(
        name=_generate_workflow_name(builder._steps),
        steps=[
            Step(name="id-generator", task="id_generator", config={"num_records": 5})
        ],
    )

    assert actual.name == expected.name
    assert actual.steps == expected.steps


def test_workflow_builder_add_step(builder: WorkflowBuilder):
    builder.add_step(
        Step(
            name="generate_ids",
            task="id_generator",
            config={},
        )
    )
    actual = builder.to_workflow().steps
    expected = [Step(name="generate_ids", task="id_generator", config={})]
    assert actual == expected


def test_workflow_task_validation(
    builder: WorkflowBuilder, tasks: Registry, api_provider_mock: TestGretelApiFactory
):
    builder.add_step(tasks.IdGenerator())
    api_provider_mock.get_mock(
        DefaultApi
    ).tasks_validate_v2_workflows_tasks_validate_post.assert_called_with(
        TaskEnvelope(
            name="id_generator",
            config=tasks.IdGenerator().model_dump(exclude_defaults=True),
        )
    )


def test_workflow_task_validation_error(
    builder: WorkflowBuilder, tasks: Registry, api_provider_mock: TestGretelApiFactory
):
    mock_response = Mock()
    mock_response.status = 422
    mock_response.body = '{"message": "Validation error", "details": [{"field_violations": [{"field": "num_records", "error_message": "Field is required"}]}]}'

    api_provider_mock.get_mock(
        DefaultApi
    ).tasks_validate_v2_workflows_tasks_validate_post.side_effect = ApiException(
        status=422, reason="Unprocessable Entity", body=mock_response.body
    )

    with pytest.raises(WorkflowValidationError) as excinfo:
        builder.add_step(tasks.IdGenerator())

    assert "Validation error" in str(excinfo.value)
    assert len(excinfo.value.field_violations) == 1
    assert excinfo.value.field_violations[0].field == "num_records"
    assert excinfo.value.field_violations[0].error_message == "Field is required"


def test_does_create_preview(
    builder: WorkflowBuilder, api_provider_mock: TestGretelApiFactory
):
    # Setup mock API response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.iter_lines.return_value = [
        json.dumps(
            {
                "step": "test_step",
                "stream": "stdout",
                "payload": {"data": "test_data"},
                "type": "output",
                "ts": datetime.datetime.now().isoformat(),
            }
        ).encode()
    ]

    # Configure the mock requests Session
    mock_session = api_provider_mock.requests()
    mock_post_return = Mock()
    # Configure the mock to support context manager protocol
    mock_post_return.__enter__ = Mock(return_value=mock_response)
    mock_post_return.__exit__ = Mock(return_value=False)
    mock_session.post.return_value = mock_post_return
    mock_session.return_value = mock_session

    # Add a step to the workflow
    builder.add_step(
        Step(name="test_step", task="id_generator", config={}), validate=False
    )

    # Call the preview method and collect results
    messages = list(builder.iter_preview())

    # Verify the API was called with correct parameters
    mock_session.post.assert_called_once()
    call_args = mock_session.post.call_args

    # Check that the request was made to the correct endpoint
    assert call_args[0][0] == "/v2/workflows/exec_streaming"

    # Verify the workflow configuration in the request
    workflow_config = call_args[1]["json"]
    assert "steps" in workflow_config
    assert len(workflow_config["steps"]) == 1
    assert workflow_config["steps"][0]["name"] == "test_step"

    # Verify the messages returned from the preview
    assert len(messages) == 1
    assert messages[0].step == "test_step"
    assert messages[0].stream == "stdout"
    assert messages[0].payload == {"data": "test_data"}
    assert messages[0].type == "output"


def test_does_submit_batch_job(
    builder: WorkflowBuilder, api_provider_mock: TestGretelApiFactory
):
    # Setup mock API response
    mock_response = create_autospec(ExecBatchResponse)
    mock_response.workflow_run_id = "wr_1"
    mock_response.workflow_id = "w_1"

    # Configure the mock API
    api_provider_mock.get_mock(
        DefaultApi
    ).workflows_exec_batch_v2_workflows_exec_batch_post.return_value = mock_response

    # Add a step to the workflow
    builder.add_step(
        Step(name="test_step", task="id_generator", config={}), validate=False
    )

    # Run the workflow
    with patch(
        "gretel_client.workflows.builder.WorkflowRun.from_workflow_run_id"
    ) as from_workflow_run_id:
        builder.run(name="test-workflow")

    from_workflow_run_id.assert_called_once_with("wr_1", ANY, ANY)

    # Verify the API was called with correct parameters
    api_provider_mock.get_mock(
        DefaultApi
    ).workflows_exec_batch_v2_workflows_exec_batch_post.assert_called_once()
    call_args = api_provider_mock.get_mock(
        DefaultApi
    ).workflows_exec_batch_v2_workflows_exec_batch_post.call_args[0][0]

    # Check that the request contains the expected data
    assert call_args.project_id == "proj_1"
    assert call_args.workflow_config.model_dump() == {
        "globals": None,
        "inputs": None,
        "name": "test-workflow",
        "steps": [
            {
                "config": {},
                "inputs": None,
                "name": "test_step",
                "task": "id_generator",
            }
        ],
        "version": "2",
    }


def test_workflow_session_management(
    api_provider_mock: TestGretelApiFactory,
    resource_provider_mock: TestGretelResourceProvider,
    tasks: Registry,
    stub_globals: Globals,
):
    workflow_session = WorkflowSessionManager()

    builder_one = WorkflowBuilder(
        "proj_1",
        stub_globals,
        api_provider_mock,
        resource_provider_mock,
        workflow_session,
    )
    builder_two = WorkflowBuilder(
        "proj_1",
        stub_globals,
        api_provider_mock,
        resource_provider_mock,
        workflow_session,
    )

    builder_one.for_workflow("w_1")

    builder_two.add_step(tasks.IdGenerator())
    builder_two.run()

    api_provider_mock.get_mock(
        DefaultApi
    ).workflows_exec_batch_v2_workflows_exec_batch_post.assert_called_once_with(
        ExecBatchRequest(
            project_id="proj_1",
            workflow_config=WorkflowInput(**builder_two.to_dict()),
            workflow_id="w_1",
        )
    )


def test_handle_step_name_duplicates(builder: WorkflowBuilder, tasks: Registry):
    builder.add_steps([tasks.IdGenerator(), tasks.IdGenerator(), tasks.Combiner()])

    actual = builder.to_workflow().steps
    expected = [
        Step(name="id-generator", task="id_generator", config={}),
        Step(
            name="id-generator-1",
            task="id_generator",
            config={},
        ),
        Step(name="combiner", task="combiner", config={}),
    ]

    assert actual == expected


def test_handle_step_name_duplicates_with_inputs(
    builder: WorkflowBuilder, tasks: Registry
):
    id_generator = tasks.IdGenerator()
    first_name_generator = tasks.NameGenerator(column_name="first_name")
    last_name_generator = tasks.NameGenerator(column_name="last_name")
    full_name_combiner = tasks.Combiner()
    profile_combiner = tasks.Combiner()

    builder.add_step(id_generator)
    builder.add_step(first_name_generator)
    builder.add_step(last_name_generator)
    builder.add_step(
        full_name_combiner, step_inputs=[first_name_generator, last_name_generator]
    )
    builder.add_step(profile_combiner, step_inputs=[id_generator, full_name_combiner])

    actual = builder.to_workflow().steps
    expected = [
        Step(name="id-generator", task="id_generator", config={}),
        Step(
            name="name-generator",
            task="name_generator",
            config={
                "column_name": "first_name",
            },
        ),
        Step(
            name="name-generator-1",
            task="name_generator",
            config={
                "column_name": "last_name",
            },
        ),
        Step(
            name="combiner",
            task="combiner",
            config={},
            inputs=["name-generator", "name-generator-1"],
        ),
        Step(
            name="combiner-1",
            task="combiner",
            config={},
            inputs=["id-generator", "combiner"],
        ),
    ]

    assert actual == expected
