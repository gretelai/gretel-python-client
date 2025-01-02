import json

from contextlib import nullcontext
from unittest.mock import Mock, patch

import pandas as pd
import pytest

import gretel_client.inference_api.base as api_base
import gretel_client.inference_api.tabular as tabular

from gretel_client.inference_api.tabular import GretelInferenceAPIError

tabular.STREAM_SLEEP_TIME = 0

# two data points to cap off the end of the stream
ENDING_STREAM_DATA = [
    {
        "data": [
            {
                "data_type": "ResponseMetadata",
                "data": json.dumps(
                    {"gretel": "a_model_id", "usage": {"input_bytes": 42}}
                ),
            }
        ]
    },
    {"stream_state": {"status": "closed"}, "data": []},
]


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
    api = tabular.TabularInferenceAPI(skip_configure_session=True)
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
    api = tabular.TabularInferenceAPI(skip_configure_session=True)

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
                    side_effect=[{"stream_id": "new_stream_123"}]
                    + [api_response] * 15
                    + ENDING_STREAM_DATA
                )
                api._last_stream_read -= timeout + 1

    if expects_error:
        return

    assert len(records) == target_count
    assert records[0] == {"foo": "bar"}

    # Because we've closed the stream, the stream_id should be reset
    assert api._curr_stream_id == None

    # We should have a history buffer that was sent as well
    stream_body = api._call_api.call_args_list[0].kwargs["body"]
    assert stream_body["ref_data"]["sample_data"]["table_headers"] == ["foo"]
    assert len(stream_body["ref_data"]["sample_data"]["table_data"]) == 5
    assert stream_body["ref_data"]["sample_data"]["table_data"][0] == {"foo": "bar"}

    # Since the last stream closed, we should be able to get the metadata
    assert api.get_response_metadata()["usage"]["input_bytes"] == 42


@patch.object(api_base, "get_full_navigator_model_list")
@patch.object(api_base, "get_model")
def test_initial_sample_data_is_sent(
    mock_models,
    mock_all_models,
):
    # We need to patch the models api calls within the class since we make an API
    # call right away to retrieve the model list
    mock_all_models.return_value = [
        {"model_id": "gretelai/auto", "model_type": "TABULAR"}
    ]
    mock_models.return_value = [
        {"model_id": "gretelai/tabular-v0", "model_type": "TABULAR"}
    ]
    api = tabular.TabularInferenceAPI(skip_configure_session=True)

    api_response = {
        "data": [
            {
                "data_type": "TabularResponse",
                "data": {"table_data": [{"foo": "bar"}]},
            }
        ]
    }

    target_count = 5

    # The first API call creates the first stream, then we
    # will return 1 record at a time simulating getting records back
    mock_data = Mock(
        side_effect=[{"stream_id": "stream123"}]
        + target_count * [api_response]
        + ENDING_STREAM_DATA
    )
    api._call_api = mock_data
    records = []

    for record in api.generate(
        prompt="make me stuff",
        num_records=target_count,
        sample_data=[{"foo": "sample"}],
        stream=True,
    ):
        records.append(record)

    assert len(records) == target_count
    assert records[0] == {"foo": "bar"}

    # Because we've closed the stream, the stream_id should be reset
    assert api._curr_stream_id == None

    # We should have a history buffer that was sent as well
    stream_body = api._call_api.call_args_list[0].kwargs["body"]
    assert stream_body["ref_data"]["sample_data"]["table_headers"] == ["foo"]
    assert len(stream_body["ref_data"]["sample_data"]["table_data"]) == 1
    assert stream_body["ref_data"]["sample_data"]["table_data"][0] == {"foo": "sample"}

    # Since the last stream closed, we should be able to get the metadata
    assert api.get_response_metadata()["usage"]["input_bytes"] == 42


def test_data_input_to_api_data_dicts():
    input_data = [
        {"foo": "bar", "number": 12},
        {"foo": "baz", "number": 33},
    ]
    headers, data = tabular._data_input_to_api_data(input_data, "Test data")

    assert headers == ["foo", "number"]
    assert data == input_data


def test_data_input_to_api_data_pandas():
    input_data = pd.DataFrame(
        {
            "foo": ["bar", "baz"],
            "number": [12, 33],
        }
    )
    headers, data = tabular._data_input_to_api_data(input_data, "Test data")

    assert headers == ["foo", "number"]
    assert data == input_data.to_dict(orient="records")


def test_data_input_to_api_data_invalid():
    with pytest.raises(GretelInferenceAPIError) as e:
        tabular._data_input_to_api_data(["foo", 123], "Test data")

    assert str(e.value) == "Test data must be a `pandas` DataFrame or a list of dicts."
