"""
This module provides an Azure OpenAI adapter for the Gretel Navigator Tabular API.

To get started, you should create your own instance of AzureOpenAI and then pass
that to the AzureOpenAIAdapter.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from gretel_client.inference_api.third_party.base import (
    BaseThirdParty,
    DataStreamT,
    get_model_id,
    handle_non_streaming_response,
    handle_streaming_chunk,
    InferenceParams,
    ResponseMetadata,
    UserReturnDataT,
)

if TYPE_CHECKING:
    from openai import AzureOpenAI, OpenAI


def _create_stream_from_oai_endpoint(
    client: OpenAI, params: InferenceParams
) -> DataStreamT:
    """
    Regardless of using actual streaming or not, we create a unified interface
    that creates a Python generator for handling results from the upstream API.
    """

    messages = [{"role": "user", "content": params.prompt}]

    response = client.chat.completions.create(
        model=params.model,
        messages=messages,  # type: ignore
        stream=params.stream,
        n=params.num_records,
        temperature=params.temperature,
        top_p=params.top_p,
        extra_body={
            "gretel": params.gretel_metadata,
        },
        extra_headers={
            "azureml-maas-model": params.model,
            "extra-parameters": "pass-through",
        },
    )

    metadata = ResponseMetadata()

    if not params.stream:
        yield from iter(handle_non_streaming_response(response.model_dump()))
        metadata = ResponseMetadata(
            response.id,
            usage=dict(response.usage),
        )
        metadata.model_id = get_model_id(response.model_dump())
    else:
        for chunk in response:
            yield from iter(handle_streaming_chunk(chunk.model_dump()))

            # last record
            if usage := chunk.usage:
                metadata = ResponseMetadata(
                    chunk.id,
                    usage=dict(usage),
                )
                metadata.model_id = get_model_id(chunk.model_dump())

    return metadata


class AzureOpenAIAdapter(BaseThirdParty):
    """
    Use Gretel's Navigator Tabular API with Azure OpenAI endpoints.

    Args:
        open_ai_client: An instance of AzureOpenAI from the `openai` package.

    Example:
        from openai import AzureOpenAI

        client = AzureOpenAI(...)
        adapter = AzureOpenAIAdapter(client)
        metadata, generated_data = adapter.generate(...)
    """

    def __init__(self, open_ai_client: AzureOpenAI):
        self._client = open_ai_client

    def _build_and_handle_stream(
        self,
        params: InferenceParams,
    ) -> UserReturnDataT:
        base_stream = _create_stream_from_oai_endpoint(self._client, params)
        return self._consume_stream(base_stream, params)
