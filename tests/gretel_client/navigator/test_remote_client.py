import pytest

from gretel_client.navigator.client.remote import Message, WorkflowTaskError


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
