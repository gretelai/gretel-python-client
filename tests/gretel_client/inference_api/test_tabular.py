from contextlib import nullcontext
from unittest.mock import Mock, patch

import pytest

import gretel_client.inference_api.base as api_base
import gretel_client.inference_api.tabular as tabular

from gretel_client.inference_api.tabular import GretelInferenceAPIError

tabular.STREAM_SLEEP_TIME = 0


@patch.object(api_base, "get_full_navigator_model_list")
@patch.object(api_base, "get_model")
def test_generate_error_retry(mock_models, mock_all_models):
    # We need to patch the models api calls within the class since we make an API
    # call right away to retrieve the model list
    mock_all_models.return_value = [
        {"model_id": "gretelai/auto", "model_type": "TABULAR"}
    ]
    mock_models.return_value = [
        {"model_id": "gretelai/tabular-v0", "model_type": "TABULAR"}
    ]
    api = tabular.TabularInferenceAPI()
    api_response = {
        "data": [
            {
                "data_type": "logger.error",
                "data": "something bad",
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

    with pytest.raises(GretelInferenceAPIError, match="3 attempts"):
        for _ in api.generate(
            prompt="make me stuff",
            num_records=target_count,
            stream=True,
            sample_buffer_size=5,
        ):
            pass


@pytest.mark.parametrize("retry_count", [2, 1])
@patch.object(api_base, "get_full_navigator_model_list")
@patch.object(api_base, "get_model")
def test_generate_timeout(
    mock_models,
    mock_all_models,
    retry_count: int,
):
    # We need to patch the models api calls within the class since we make an API
    # call right away to retrieve the model list
    mock_all_models.return_value = [
        {"model_id": "gretelai/auto", "model_type": "TABULAR"}
    ]
    mock_models.return_value = [
        {"model_id": "gretelai/tabular-v0", "model_type": "TABULAR"}
    ]
    api = tabular.TabularInferenceAPI()

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
    api.max_retry_count = retry_count
    records = []

    expects_error = False

    # Since this test forces a mock time out, if the retry_count
    # is set to 1, then we'll raise an error because of too
    # many attempts, so we adapt the test to check for this error
    # when our retry_count is only 1

    if retry_count == 2:
        context = nullcontext()
    else:
        context = pytest.raises(GretelInferenceAPIError)
        expects_error = True

    with context:
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

    if expects_error:
        return

    assert len(records) == target_count
    assert records[0] == {"foo": "bar"}
    assert api._curr_stream_id == "new_stream_123"

    # We should have a history buffer that was sent as well
    stream_body = api._call_api.call_args_list[0].kwargs["body"]
    assert stream_body["ref_data"]["sample_data"]["table_headers"] == ["foo"]
    assert len(stream_body["ref_data"]["sample_data"]["table_data"]) == 5
    assert stream_body["ref_data"]["sample_data"]["table_data"][0] == {"foo": "bar"}
