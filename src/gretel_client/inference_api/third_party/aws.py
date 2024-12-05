"""
This module provides an AWS SageMaker adapter for the Gretel Navigator Tabular API.

To get started, you should create your own instance of SageMakerRuntimeClient and then pass
that to the SagemakerAdapter.
"""

from __future__ import annotations

import json

from io import StringIO
from typing import TYPE_CHECKING

# TODO: Check for boto3 once we have the minimal version that will be needed. Maybe just
# enforce it at the class level (on init).

if TYPE_CHECKING:
    from mypy_boto3_sagemaker_runtime import SageMakerRuntimeClient
    from botocore.client import BaseClient

from gretel_client.inference_api.third_party.base import (
    BaseThirdParty,
    DataStreamT,
    get_model_id,
    handle_non_streaming_response,
    handle_streaming_chunk,
    InferenceParams,
    maybe_parse_last_chunk,
    ResponseMetadata,
    UserReturnDataT,
)


def _create_stream_from_bedrock(
    client: BaseClient, endpoint_arn: str, params: InferenceParams
) -> DataStreamT:

    payload = params.as_openai_payload
    metadata = ResponseMetadata()

    if not params.stream:
        response = client.invoke_model(  # type: ignore
            modelId=endpoint_arn,
            body=json.dumps(payload),
        )
        response_dict = json.load(response["body"])
        yield from iter(handle_non_streaming_response(response_dict))
        metadata.completion_id = response_dict.get("id")
        metadata.usage = response_dict.get("usage")
        metadata.model_id = get_model_id(response_dict)
    else:
        response = client.invoke_model_with_response_stream(modelId=endpoint_arn, body=json.dumps(payload))  # type: ignore
        event_stream = response["body"]
        chunk_buffer = StringIO()
        for event in event_stream:
            event_str = event["chunk"]["bytes"].decode("utf-8")  # type: ignore
            # event_str = event_str.removeprefix("data: ").strip()
            chunk_buffer.write(event_str)
            try:
                event_dict = json.loads(chunk_buffer.getvalue())
                yield from iter(handle_streaming_chunk(event_dict))
                last_record_info = maybe_parse_last_chunk(event_dict)
                if last_record_info is not None:
                    response_id, usage, model_id = last_record_info
                    metadata.completion_id = response_id
                    metadata.usage = usage
                    metadata.model_id = model_id
                chunk_buffer = StringIO()
            except json.JSONDecodeError:
                continue

    return metadata


def _create_stream_from_sagemaker(
    client: SageMakerRuntimeClient, endpoint_name: str, params: InferenceParams
) -> DataStreamT:

    payload = params.as_openai_payload
    metadata = ResponseMetadata()

    if not params.stream:
        response = client.invoke_endpoint(
            EndpointName=endpoint_name,
            Body=json.dumps(payload),
            ContentType="application/json",
            Accept="application/json",
        )
        response_dict = json.load(response["Body"])
        yield from iter(handle_non_streaming_response(response_dict))
        metadata.completion_id = response_dict.get("id")
        metadata.usage = response_dict.get("usage")
        metadata.model_id = get_model_id(response_dict)
    else:
        response = client.invoke_endpoint_with_response_stream(
            EndpointName=endpoint_name,
            Body=json.dumps(payload),
            ContentType="application/json",
            Accept="application/json",
        )
        event_stream = response["Body"]
        chunk_buffer = StringIO()
        for event in event_stream:
            event_str = event["PayloadPart"]["Bytes"].decode("utf-8")  # type: ignore
            event_str = event_str.removeprefix("data: ").strip()
            chunk_buffer.write(event_str)
            try:
                event_dict = json.loads(chunk_buffer.getvalue())
                yield from iter(handle_streaming_chunk(event_dict))
                last_record_info = maybe_parse_last_chunk(event_dict)
                if last_record_info is not None:
                    response_id, usage, model_id = last_record_info
                    metadata.completion_id = response_id
                    metadata.usage = usage
                    metadata.model_id = model_id
                chunk_buffer = StringIO()
            except json.JSONDecodeError:
                continue

    return metadata


class BedrockAdapter(BaseThirdParty):
    """
    Use Gretel's Navigator Tabular API with Bedrock endpoints.

    Args:
        client: An instance of the bedrock client from the `boto3` package.
        endpoint_arn: The ARN of the Bedrock (Sagemaker) endpoint.

    Example:

        client = boto3.client("bedrock-runtime")
        adapter = BedrockAdapter(client, "my-endpoint-arn")
        metadata, generated_data = adapter.generate(...)
    """

    def __init__(self, client: BaseClient, endpoint_arn: str):
        self._client = client
        self._endpoint_arn = endpoint_arn

    def _build_and_handle_stream(
        self,
        params,
    ) -> UserReturnDataT:

        base_stream = _create_stream_from_bedrock(
            self._client, self._endpoint_arn, params
        )
        return self._consume_stream(base_stream, params)


class SagemakerAdapter(BaseThirdParty):
    """
    Use Gretel's Navigator Tabular API with AWS SageMaker endpoints.

    Args:
        sagemaker_client: An instance of SageMakerRuntimeClient from the `mypy_boto3_sagemaker_runtime` package.
        endpoint_name: The name of the SageMaker endpoint.

    Example:
        from mypy_boto3_sagemaker_runtime import SageMakerRuntimeClient

        client = boto3.client("sagemaker-runtime")
        adapter = SagemakerAdapter(client, "my-endpoint-name")
        metadata, generated_data = adapter.generate(...)
    """

    def __init__(self, sagemaker_client: SageMakerRuntimeClient, endpoint_name: str):
        self._client = sagemaker_client
        self._endpoint_name = endpoint_name

    def _build_and_handle_stream(
        self,
        params,
    ) -> UserReturnDataT:

        base_stream = _create_stream_from_sagemaker(
            self._client, self._endpoint_name, params
        )
        return self._consume_stream(base_stream, params)
