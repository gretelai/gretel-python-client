import time

from unittest.mock import Mock, patch

import gretel_client.inference_api.tabular as tabular

tabular.STREAM_SLEEP_TIME = 0


def test_generate_timeout():
    # We need to patch the api call method on the class since we make an API
    # call right away to retrieve the model list
    with patch.object(tabular.TabularLLMInferenceAPI, "_call_api") as mock_models:
        mock_models.return_value = {
            "models": [{"model_id": "gretelai/tabular-v0", "model_type": "TABULAR"}]
        }
        api = tabular.TabularLLMInferenceAPI()

    timeout = 60
    api_response = {
        "data": [
            {
                "data_type": "TabularResponse",
                "data": {"table_data": [{"foo": "bar"}]},
            }
        ]
    }

    target_count = 20

    # The first API call creates the first stream, then we
    # will return 1 record at a time simulating getting records back
    mock_data = Mock(
        side_effect=[{"stream_id": "stream123"}] + target_count * [api_response]
    )
    api._call_api = mock_data
    records = []
    for record in api.generate(
        prompt="make me stuff",
        num_records=target_count,
        stream=True,
        sample_buffer_size=5,
    ):
        records.append(record)

        # Here we simulate the stream timing out simply by
        # forcing the last read timestamp to trigger the
        # time out, which should force a new stream
        # to get created.
        #
        # The updated mock API return value will set
        # this new stream ID which is what we assert
        if len(records) == target_count - 5:
            api._call_api = Mock(
                side_effect=[{"stream_id": "new_stream_123"}] + [api_response] * 15
            )
            api._last_stream_read -= timeout + 1

    assert len(records) == target_count
    assert records[0] == {"foo": "bar"}
    assert api._curr_stream_id == "new_stream_123"

    # We should have a history buffer that was sent as well
    stream_body = api._call_api.call_args_list[0].kwargs["body"]
    assert stream_body["ref_data"]["sample_data"]["table_headers"] == ["foo"]
    assert len(stream_body["ref_data"]["sample_data"]["table_data"]) == 5
    assert stream_body["ref_data"]["sample_data"]["table_data"][0] == {"foo": "bar"}
