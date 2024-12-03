from unittest.mock import MagicMock, patch

import pytest

from gretel_client.navigator.client.remote import (
    Message,
    RemoteClient,
    WorkflowTaskError,
)


def test_does_raise_on_task_error():
    with pytest.raises(WorkflowTaskError) as e:
        Message.from_dict(
            {
                "step": "generate-dataset-from-sample-records-2",
                "ts": "2024-11-21T19:02:33.360388",
                "type": "step_state_change",
                "stream": "logs",
                "payload": {
                    "state": "error",
                    "msg": "\\ud83d\\uded1 Max retries exceeded",
                },
            },
            raise_on_error=True,
        )
    assert "Max retries exceeded" in str(e)


@patch("gretel_client.navigator.client.remote.requests.post")
@patch("gretel_client.navigator.client.remote.Gretel")
def test_does_pass_batch_params(mock_gretel, mock_post):

    gretel_instance = MagicMock()
    mock_gretel.return_value = gretel_instance
    gretel_instance.get_project.return_value = MagicMock(project_guid="proj_1")

    client_config = MagicMock()
    client_config.api_key = "grtu-test"
    client = RemoteClient(
        api_endpoint="http://test.endpoint", client_session=client_config
    )

    client.submit_batch_workflow({"steps": []}, 100, "test-project", "w_1")

    gretel_instance.set_project.assert_called_once_with(name="test-project")
    gretel_instance.get_project.assert_called_once()

    mock_post.assert_any_call(
        "http://test.endpoint/v2/workflows/exec_batch",
        json={
            "workflow_config": {"steps": []},
            "project_id": "proj_1",
            "workflow_id": "w_1",
        },
        headers={"Authorization": "grtu-test"},
    )
