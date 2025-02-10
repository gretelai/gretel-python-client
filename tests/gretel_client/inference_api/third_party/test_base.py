import pandas as pd

from gretel_client.inference_api.third_party.base import (
    get_model_id,
    handle_non_streaming_response,
    handle_streaming_chunk,
    InferenceParams,
    maybe_parse_last_chunk,
    NavigatorDataStream,
    ResponseMetadata,
)


def _simple_generator():
    yield from iter([{"a": 1}, {"b": 2}])
    return ResponseMetadata()


def test_navigator_data_stream():
    stream = NavigatorDataStream(_simple_generator())
    records = [record for record in stream]
    assert records == [{"a": 1}, {"b": 2}]
    assert stream.metadata == ResponseMetadata()


def test_inference_params():
    sample_data = {
        "table_headers": [
            "first_name",
            "last_name",
            "email",
            "gender",
            "city",
            "country",
        ],
        "table_data": [
            {
                "first_name": "Lea",
                "last_name": "Martin",
                "email": "lmartin@foo.io",
                "gender": "Female",
                "city": "Lyon",
                "country": "France",
            }
        ],
    }
    expected = {
        "messages": [
            {
                "role": "user",
                "content": "prompty mcprompt face",
            }
        ],
        "model": "gretelai-azure/gpt-4o-mini",
        "n": 20,
        "stream": True,
        "temperature": None,
        "top_p": None,
        "gretel": {"ref_data": {"sample_data": sample_data}},
    }

    params = InferenceParams(
        model="gretelai-azure/gpt-4o-mini",
        prompt="prompty mcprompt face",
        num_records=20,
        temperature=None,
        top_p=None,
        stream=True,
        as_dataframe=True,
        disable_progress_bar=False,
        pbar_desc="foo",
        sample_data=pd.DataFrame(
            sample_data["table_data"], columns=sample_data["table_headers"]
        ),
    )

    assert params.as_openai_payload == expected


def test_handle_non_streaming_response():
    data = {
        "id": "6c6b7b14-e778-48b9-9ff9-59ace6f95de7",
        "choices": [
            {
                "finish_reason": "stop",
                "index": 0,
                "logprobs": None,
                "message": {
                    "content": (
                        '{"table_headers":["first_name","last_name","email","gender","city","country"],'
                        '"table_data":[{"first_name":"Luc","last_name":"Dubois","email":"ldubois@foo.io","gender":"Male","city":"Paris","country":"France"}]}\n'
                        '{"table_headers":["first_name","last_name","email","gender","city","country"],'
                        '"table_data":[{"first_name":"Aurélie","last_name":"Lefevre","email":"alefevre@foo.io","gender":"Female","city":"Marseille","country":"France"}]}\n'
                        '{"table_headers":["first_name","last_name","email","gender","city","country"],'
                        '"table_data":[{"first_name":"Théo","last_name":"Girard","email":"tgirard@foo.io","gender":"Male","city":"Toulouse","country":"France"}]}\n'
                    ),
                    "refusal": None,
                    "role": "assistant",
                    "function_call": None,
                    "tool_calls": None,
                },
            }
        ],
        "created": 1730499763,
        "model": "gretelai-azure/gpt-4o-mini",
        "object": "chat.completion",
        "service_tier": None,
        "system_fingerprint": None,
        "usage": {
            "completion_tokens": 538,
            "prompt_tokens": 99,
            "total_tokens": 637,
            "completion_tokens_details": None,
        },
        "gretel": {"model_id": "themodel"},
    }

    records = handle_non_streaming_response(data)
    assert records == [
        {
            "first_name": "Luc",
            "last_name": "Dubois",
            "email": "ldubois@foo.io",
            "gender": "Male",
            "city": "Paris",
            "country": "France",
        },
        {
            "first_name": "Aurélie",
            "last_name": "Lefevre",
            "email": "alefevre@foo.io",
            "gender": "Female",
            "city": "Marseille",
            "country": "France",
        },
        {
            "first_name": "Théo",
            "last_name": "Girard",
            "email": "tgirard@foo.io",
            "gender": "Male",
            "city": "Toulouse",
            "country": "France",
        },
    ]

    assert get_model_id(data) == "themodel"


def test_handle_streaming_chunk():
    data = {
        "id": "109a0b84-7edb-4d92-8726-16020aa60e02",
        "choices": [
            {
                "delta": {
                    "content": '{"table_headers":["first_name","last_name","email","gender","city","country"],"table_data":[{"first_name":"Étienne","last_name":"Lefèvre","email":"elefevre@foo.io","gender":"Male","city":"Paris","country":"France"}]}',
                    "function_call": None,
                    "refusal": None,
                    "role": "assistant",
                    "tool_calls": None,
                },
                "finish_reason": None,
                "index": 0,
                "logprobs": None,
            }
        ],
        "created": 1730500462,
        "model": "gretelai-azure/gpt-4o-mini",
        "object": "chat.completion.chunk",
        "service_tier": None,
        "system_fingerprint": None,
        "usage": None,
    }

    records = handle_streaming_chunk(data)
    assert records == [
        {
            "first_name": "Étienne",
            "last_name": "Lefèvre",
            "email": "elefevre@foo.io",
            "gender": "Male",
            "city": "Paris",
            "country": "France",
        }
    ]

    last_chunk = {
        "id": "109a0b84-7edb-4d92-8726-16020aa60e02",
        "choices": [
            {
                "delta": {
                    "content": None,
                    "function_call": None,
                    "refusal": None,
                    "role": None,
                    "tool_calls": None,
                },
                "finish_reason": None,
                "index": 0,
                "logprobs": None,
            }
        ],
        "created": 1730500462,
        "model": "gretelai-azure/gpt-4o-mini",
        "object": "chat.completion.chunk",
        "service_tier": None,
        "system_fingerprint": None,
        "usage": {
            "completion_tokens": 542,
            "prompt_tokens": 99,
            "total_tokens": 641,
            "completion_tokens_details": None,
            "input_bytes": 399,
            "output_bytes": 2169,
            "total_bytes": 2568,
            "billed_bytes": 2570,
            "billed_credits": 0.0257,
        },
        "gretel": {"model_id": "themodel"},
    }

    assert not maybe_parse_last_chunk(data)
    id, usage, model_id = maybe_parse_last_chunk(last_chunk)  # type: ignore
    assert id == "109a0b84-7edb-4d92-8726-16020aa60e02"
    assert usage == {
        "completion_tokens": 542,
        "prompt_tokens": 99,
        "total_tokens": 641,
        "completion_tokens_details": None,
        "input_bytes": 399,
        "output_bytes": 2169,
        "total_bytes": 2568,
        "billed_bytes": 2570,
        "billed_credits": 0.0257,
    }
    assert model_id == "themodel"
