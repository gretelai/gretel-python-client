"""
This module provides an interface for using Gretel Navigator with third party APIs.
"""

import json

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Generator, List, Optional, Tuple, Union

import pandas as pd

from tqdm import tqdm

from gretel_client.dataframe import _DataFrameT
from gretel_client.gretel.config_setup import NavigatorDefaultParams
from gretel_client.inference_api.tabular import PROGRESS_BAR_FORMAT

UserDataT = Union[_DataFrameT, List[dict[str, Any]]]


@dataclass
class ResponseMetadata:
    completion_id: Optional[str] = None
    usage: dict[str, Any] = field(default_factory=dict)
    model_id: Optional[str] = None


DataStreamT = Generator[dict[str, Any], None, ResponseMetadata]


class NavigatorDataStream:
    """
    Returned from concrete generation and edit API calls. Should not instantiate directly.

    This class operates as a standard Python generator with the exception that after
    the generated is exhausted, metadata is stored and made available on a special
    property of the instance.

    You should be able to iterate over this class instance as you would any other generator
    and then access the metadata.

    Example:
        data_stream: NavigatorDataStream = ... # API that returns an instance
        for record in data_stream:
            print(record)
        print(data_stream.metadata)
    """

    metadata: Optional[ResponseMetadata]

    def __init__(self, stream: DataStreamT):
        self.stream = stream
        self.metadata = None

    def __iter__(self):
        while True:
            try:
                yield next(self.stream)
            except StopIteration as e:
                self.metadata = e.value
                break


UserReturnDataT = Union[
    NavigatorDataStream, Tuple[Optional[ResponseMetadata], UserDataT]
]


@dataclass
class InferenceParams:
    model: str
    prompt: str
    num_records: int
    """The number of records to generate or edit."""
    temperature: Optional[float]
    top_p: Optional[float]
    stream: bool
    as_dataframe: bool
    disable_progress_bar: bool
    pbar_desc: str
    sample_data: Optional[UserDataT] = None
    """Generate mode only, example data to guide data generation."""
    edit_data: Optional[UserDataT] = None
    """Edit mode only, the data to be edited."""

    @property
    def gretel_metadata(self) -> dict[str, Any]:
        """
        Create the `gretel` object that gets added as extra parameters
        to OAI compatible requests.
        """
        meta = {}

        # construct ref data if we were provided any
        ref_data = {}
        if self.sample_data is not None:
            ref_data["sample_data"] = _user_data_to_ref_data(self.sample_data)
        if self.edit_data is not None:
            ref_data["data"] = _user_data_to_ref_data(self.edit_data)
        if ref_data:
            meta["ref_data"] = ref_data

        return meta

    @property
    def as_openai_payload(self) -> dict[str, Any]:
        """
        Creates a dictionary that can be sent to OAI
        compatible endpoints when using the openai package
        is not possible.
        """
        return {
            "model": self.model,
            "n": self.num_records,
            "stream": self.stream,
            "messages": [{"role": "user", "content": self.prompt}],
            "temperature": self.temperature,
            "top_p": self.top_p,
            "gretel": self.gretel_metadata,
        }


class BaseThirdParty(ABC):
    """
    Define interfaces that third party adapters should follow.
    """

    @abstractmethod
    def _build_and_handle_stream(
        self,
        params: InferenceParams,
    ) -> UserReturnDataT: ...

    def _consume_stream(
        self, base_stream: DataStreamT, params: InferenceParams
    ) -> UserReturnDataT:
        data_stream = NavigatorDataStream(base_stream)

        # We only return a Python generator if streaming is enabled
        # and the user has NOT requested a DataFrame. If a DataFrame
        # is requested, we can't stream a DF back effectively so we
        # will need to exhaust the stream to build up the full DataFrame.
        if params.stream and not params.as_dataframe:
            return data_stream

        return consume_stream(data_stream, params)

    def generate(
        self,
        model: str,
        prompt: str,
        *,
        num_records: int,
        sample_data: Optional[UserDataT] = None,
        temperature: float = NavigatorDefaultParams.temperature,
        top_p: float = NavigatorDefaultParams.top_p,
        stream: bool = True,
        as_dataframe: bool = True,
        disable_progress_bar: bool = False,
    ) -> UserReturnDataT:
        """Generate synthetic data from a third party service hosting Gretel Navigator. This method
        should be used after a base adapter class is instantiated with the appropriate third party client.

        Args:
            model: The model to use for generating synthetic data.
            prompt: The prompt for generating synthetic tabular data.
            num_records: The number of records to generate.
            sample_data: Example data to guide data generation. Can be a list of dicts or a DataFrame.
            temperature: Sampling temperature. Higher values make output more random.
            top_p: The cumulative probability cutoff for sampling tokens.
            stream: If True, the data will be streamed back as it is generated. If False, the data will be
                accumulated and returned all at once.
                NOTE: If `as_dataframe` is True and `stream` is True, data will still be streamed but accumulated into a DataFrame.
                If the progress bar is enabled, then it will show the progress of the record accumulation.
                Fundamentally, with the `as_dataframe=True` option, a DataFrame will not be returned until all records have been generated.
            as_dataframe: If True, the data will be returned as a DataFrame. If False, the data will be returned as a list of dicts.
                If `stream` is True, this parameter is ignored and a `NavigatorDataStream` instance is returned which can be
                iterated over to access the generated data.
            disable_progress_bar: If True, the progress bar will be disabled.
        """

        params = InferenceParams(
            model=model,
            prompt=prompt,
            num_records=num_records,
            temperature=temperature,
            top_p=top_p,
            stream=stream,
            as_dataframe=as_dataframe,
            disable_progress_bar=disable_progress_bar,
            pbar_desc="Generating data",
            sample_data=sample_data,
        )

        return self._build_and_handle_stream(params)

    def edit(
        self,
        model: str,
        prompt: str,
        *,
        seed_data: UserDataT,
        max_records: int = 100,
        temperature: float = NavigatorDefaultParams.temperature,
        top_p: float = NavigatorDefaultParams.top_p,
        stream: bool = True,
        as_dataframe: bool = True,
        disable_progress_bar: bool = False,
    ) -> UserReturnDataT:
        """Given an existing data table, edit the data based on the user-provided prompt.

        Args:
            model: The model to use for generating synthetic data.
            prompt: The prompt for generating synthetic tabular data.
            seed_data: The data to be edited. Can be a list of dicts or a DataFrame.
            max_records: The maximum number of records that can be sent to the API for editing.
            temperature: Sampling temperature. Higher values make output more random.
            top_p: The cumulative probability cutoff for sampling tokens.
            stream: If True, the data will be streamed back as it is generated. If False, the data will be
                accumulated and returned all at once.
                NOTE: If `as_dataframe` is True and `stream` is True, data will still be streamed but accumulated into a DataFrame.
                If the progress bar is enabled, then it will show the progress of the record accumulation.
                Fundamentally, with the `as_dataframe=True` option, a DataFrame will not be returned until all records have been generated.
            as_dataframe: If True, the data will be returned as a DataFrame. If False, the data will be returned as a list of dicts.
                If `stream` is True, this parameter is ignored and a `NavigatorDataStream` instance is returned which can be
                iterated over to access the generated data.
            disable_progress_bar: If True, the progress bar will be disabled.
        """

        if len(seed_data) > max_records:
            raise ValueError(
                f"Seed data length {len(seed_data)} exceeds max record size of {max_records}"
            )

        params = InferenceParams(
            model=model,
            prompt=prompt,
            num_records=len(seed_data),
            temperature=temperature,
            top_p=top_p,
            stream=stream,
            as_dataframe=as_dataframe,
            disable_progress_bar=disable_progress_bar,
            pbar_desc="Editing data",
            edit_data=seed_data,
        )

        return self._build_and_handle_stream(params)


def _user_data_to_ref_data(data: UserDataT) -> dict[str, Any]:
    if isinstance(data, pd.DataFrame):
        user_data = data.to_dict(orient="records")
    else:
        user_data = data

    return {"table_headers": list(user_data[0].keys()), "table_data": user_data}


def consume_stream(
    stream: NavigatorDataStream, params: InferenceParams
) -> Tuple[Optional[ResponseMetadata], UserDataT]:

    generated_records: list[dict[str, Any]] = []
    with tqdm(
        total=params.num_records,
        desc=params.pbar_desc,
        disable=params.disable_progress_bar,
        unit=" records",
        bar_format=PROGRESS_BAR_FORMAT,
    ) as pbar:
        for record in stream:
            generated_records.append(record)
            pbar.update(1)

    if params.as_dataframe:
        return stream.metadata, pd.DataFrame(generated_records)
    return stream.metadata, generated_records


# These parsers are for OAI style messages but operate on
# raw dicts for supporting API calls that are not made
# with the openai Python package and thus the specific
# types are not available.


def get_model_id(response: dict) -> Optional[str]:
    return response.get("gretel", {}).get("model_id")


def handle_non_streaming_response(
    response: dict,
) -> list[dict[str, Any]]:
    jsonl_str: str = (
        response.get("choices", [{}])[0].get("message", {}).get("content", "")
    )
    jsonl_chunks = jsonl_str.split("\n")
    all_records = []
    for chunk in jsonl_chunks:
        chunk = chunk.rstrip("\n")
        if chunk:
            all_records.extend(json.loads(chunk)["table_data"])
    return all_records


def handle_streaming_chunk(chunk: dict) -> list[dict[str, Any]]:
    if content := chunk.get("choices", [{}])[0].get("delta", {}).get("content"):
        response_dict = json.loads(content)
        return response_dict["table_data"]
    return []


def maybe_parse_last_chunk(chunk: dict) -> Optional[tuple[str, dict, Optional[str]]]:
    # returns: id, usage, model_id
    if usage := chunk.get("usage"):
        return chunk["id"], usage, get_model_id(chunk)
    return None


# End OAI Parsers
