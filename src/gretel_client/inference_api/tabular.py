import copy
import json
import logging
import sys
import time

from typing import Any, Iterator, List, Optional, Union

from tqdm import tqdm

from gretel_client.dataframe import _DataFrameT
from gretel_client.gretel.artifact_fetching import PANDAS_IS_INSTALLED
from gretel_client.gretel.config_setup import TabLLMDefaultParams
from gretel_client.inference_api.base import BaseInferenceAPI, GretelInferenceAPIError

if PANDAS_IS_INSTALLED:
    import pandas as pd

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

STREAM_SLEEP_TIME = 0.5
MAX_ROWS_PER_STREAM = 50
REQUEST_TIMEOUT_SEC = 60
TABULAR_API_PATH = "/v1/inference/tabular/"
TABLLM_DEFAULT_MODEL = "gretelai/tabular-v0"


StreamReturnType = Union[
    Iterator[dict[str, Any]], _DataFrameT, List[dict[str, Any]], dict[str, Any]
]


class TabularLLMInferenceAPI(BaseInferenceAPI):
    """Inference API for real-time data generation with Gretel's tabular LLM.

    Args:
        backend_model (str, optional): The model that is used under the hood.
            See the `backend_model_list` property for a list of available models.
        **session_kwargs: kwargs for your Gretel session.

    Raises:
        GretelInferenceAPIError: If the specified backend model is not valid.
    """

    _curr_stream_id: Optional[str]
    _last_stream_read: Optional[float]
    _next_iter: Optional[str]
    _generated_count: int

    request_timeout_sec: int = REQUEST_TIMEOUT_SEC
    """
    When generating data, if a request does not return data records in N seconds
    a new request will automatically be made to continue record generation.
    """

    def __init__(self, backend_model: str = "gretelai/tabular-v0", **session_kwargs):
        super().__init__(**session_kwargs)
        self.backend_model = backend_model

    @property
    def api_path(self) -> str:
        return TABULAR_API_PATH

    @property
    def stream_api_path(self) -> str:
        return self.api_path + "stream"

    @property
    def iterate_api_path(self) -> str:
        return self.api_path + "stream/iterate"

    @property
    def generate_api_path(self) -> str:
        return self.api_path + "generate"

    @property
    def backend_model(self) -> str:
        return self._backend_model

    @backend_model.setter
    def backend_model(self, backend_model: str) -> None:
        if backend_model not in self.backend_model_list:
            raise GretelInferenceAPIError(
                f"Model {backend_model} is not a valid backend model. "
                f"Valid models are: {self.backend_model_list}"
            )
        self._backend_model = backend_model
        logger.info("Backend model: %s", backend_model)
        self._reset_stream()

    @property
    def backend_model_list(self) -> List[str]:
        """Returns list of available tabular backend models."""
        return [
            m["model_id"]
            for m in self._available_backend_models
            if m["model_type"].upper() == "TABULAR"
        ]

    @property
    def name(self) -> str:
        """Returns display name for this inference api."""
        return "Gretel Tabular LLM"

    def _reset_stream(self) -> None:
        """Reset the stream state."""
        self._curr_stream_id = None
        self._last_stream_read = None
        self._next_iter = None
        self._generated_count = 0

    def _create_stream_if_needed(
        self,
        prompt: str,
        *,
        num_records: int,
        params: dict,
        ref_data: Optional[dict] = None,
    ) -> None:
        """Create a new stream if the stream id is not set.

        Args:
            prompt: The prompt for generating synthetic data.
            num_records: The number of records to generate.
            params: Additional parameters for the model's generation method.
            ref_data: Flexible option for passing additional data sources.
        """
        if self._curr_stream_id is None:
            payload = {
                "model_id": self.backend_model,
                "num_rows": min(
                    MAX_ROWS_PER_STREAM, num_records - self._generated_count
                ),
                "params": params,
                "prompt": prompt,
            }
            if ref_data is not None:
                payload["ref_data"] = ref_data
            resp = self._call_api(
                method="post", path=self.stream_api_path, body=payload
            )
            self._curr_stream_id = resp.get("stream_id")
            self._last_stream_read = time.time()
            self._next_iter = None

    def _stream(
        self,
        prompt: str,
        *,
        num_records: int,
        params: dict,
        ref_data: Optional[dict] = None,
        sample_buffer_size: int = 0,
    ) -> Iterator[dict[str, Any]]:
        """Stream data generation with tabular LLM.

        Args:
            prompt: The prompt for generating synthetic data.
            num_records: The number of records to generate.
            params: Additional parameters for the model's generation method.
            ref_data: Flexible option for passing additional data sources.
            sample_buffer_size: How many of the last N generated records that should be provided
                as sample data to subsequent generation requests.

        Raises
            GretelInferenceAPIError: If the stream is closed with an error.

        Yields:
            The generated data records.
        """
        # stores the last N records generated, only for generate mode
        history_buffer: list[dict] = []

        # references to ref data should only use our deep copied var
        this_ref_data = copy.deepcopy(ref_data) if ref_data else {}

        if sample_buffer_size > 0 and this_ref_data.get("data") is not None:
            raise ValueError("Cannot use a history buffer with data editing mode")

        self._reset_stream()
        done_generation = False

        while self._generated_count < num_records:
            if history_buffer:
                this_ref_data["sample_data"] = {
                    "table_headers": list(history_buffer[0].keys()),
                    "table_data": copy.deepcopy(history_buffer),
                }
            self._create_stream_if_needed(
                prompt=prompt,
                num_records=num_records,
                params=params,
                ref_data=this_ref_data,
            )

            # Poll the stream for new records.
            resp = self._call_api(
                method="post",
                path=self.iterate_api_path,
                body={
                    "count": MAX_ROWS_PER_STREAM,
                    "iterator": self._next_iter,
                    "stream_id": self._curr_stream_id,
                },
            )

            # Extra defensive in case the data key is missing.
            if (data_list := resp.get("data")) is None:
                logger.warning("No data returned from stream.")
                continue

            # Iterate over the records in the stream.
            for record in data_list:
                if record["data_type"] == "logger.info":
                    logger.debug("%s: %s", self.__class__.__name__, record["data"])
                elif record["data_type"] == "TabularResponse":
                    row_data = record["data"]["table_data"]
                    for row in row_data:
                        self._generated_count += 1
                        self._last_stream_read = time.time()
                        if sample_buffer_size > 0:
                            history_buffer.append(row)
                            history_buffer = history_buffer[-sample_buffer_size:]
                        yield row
                    if self._generated_count >= num_records:
                        done_generation = True
                        break
                elif record["data_type"] == "logger.error":
                    raise GretelInferenceAPIError(record["data"])

            # The stream is exhausted when the state is closed and there's no more data.
            if resp.get("stream_state", {}).get("status") == "closed" and not data_list:
                self._curr_stream_id = None
            else:
                self._next_iter = resp.get("next_iterator")

            # If we're not done generating and we hit our timeout, we'll
            # bail on using this stream.
            if (
                not done_generation
                and self._last_stream_read
                and (time.time() - self._last_stream_read) > self.request_timeout_sec
            ):
                logger.warning(
                    "Stream %s timed out after %d seconds, another stream will be used.",
                    self._curr_stream_id,
                    self.request_timeout_sec,
                )
                self._curr_stream_id = None

            time.sleep(STREAM_SLEEP_TIME)

    def _get_stream_results(
        self,
        stream_iterator: Iterator[dict[str, Any]],
        num_records: int,
        is_streaming: bool,
        as_dataframe: bool,
        pbar_desc: str,
        disable_pbar: bool,
    ) -> StreamReturnType:
        """Select and return the results for the stream.

        Args:
            stream_iterator: The stream iterator.
            num_records: Number of records to be generated.
            is_streaming: If True, return the stream iterator.
            as_dataframe: If True, return the data as a pandas DataFrame.
            pbar_desc: Description for the progress bar. Only used if not streaming.
            disable_pbar: If True, disable progress bar. Ignored if `stream` is True.

        Returns:
            The stream iterator or the generated data records.
        """
        if is_streaming:
            return stream_iterator

        generated_records = []
        with tqdm(
            total=num_records, desc=pbar_desc, disable=disable_pbar, initial=1
        ) as pbar:
            for record in stream_iterator:
                generated_records.append(record)
                pbar.update(1)

        if PANDAS_IS_INSTALLED and as_dataframe:
            return pd.DataFrame(generated_records)
        else:
            if as_dataframe:
                logger.error(
                    "Pandas is required to return dataframes. "
                    "Install using `pip install pandas`."
                )
            return generated_records

    def edit(
        self,
        prompt: str,
        *,
        seed_data: Union[_DataFrameT, List[dict[str, Any]]],
        chunk_size: int = 25,
        temperature: float = TabLLMDefaultParams.temperature,
        top_k: int = TabLLMDefaultParams.top_k,
        top_p: float = TabLLMDefaultParams.top_p,
        stream: bool = False,
        as_dataframe: bool = True,
        disable_progress_bar: bool = False,
    ) -> StreamReturnType:
        """Edit the seed data according to the given prompt.


        Args:
            prompt: A prompt specifying how to edit the seed data.
            seed_data: The seed data to edit. Must be a `pandas` DataFrame or a
                list of dicts (see format in the example below).
            chunk_size: The seed data will be divided up into chunks of this size. Each
                chunk will receive its own upstream request to be edited.
            temperature: Sampling temperature. Higher values make output more random.
            top_k: Number of highest probability tokens to keep for top-k filtering.
            top_p: The cumulative probability cutoff for sampling tokens.
            stream: If True, stream the generated data.
            as_dataframe: If True, return the data as a pandas DataFrame. This
                parameter is ignored if `stream` is True.
            disable_progress_bar: If True, disable progress bar.
                Ignored if `stream` is True.


        Raises:
            GretelInferenceAPIError: If the seed data is an invalid type.

        Returns:
            The stream iterator or the generated data records.

        Example::

            from gretel_client.inference_api.tabular import TabularLLMInferenceAPI

            # Example seed data if using a list of dicts.
            # You can also use a pandas DataFrame.
            seed_data = [
                {
                    "first_name": "Homer",
                    "last_name": "Simpson",
                    "favorite_band": "The Rolling Stones",
                    "favorite_tv_show": "Breaking Bad",
                },
                {
                    "first_name": "Marge",
                    "last_name": "Simpson",
                    "favorite_band": "The Beatles",
                    "favorite_tv_show": "Friends",
                }
            ]

            prompt = "Please add a column with the character's favorite food."

            tabllm = TabularLLMInferenceAPI(api_key="prompt")

            df = tabllm.edit(prompt=prompt, seed_data=seed_data)
        """
        if isinstance(seed_data, list) and isinstance(seed_data[0], dict):
            table_headers = list(seed_data[0].keys())
            table_data = seed_data
        elif PANDAS_IS_INSTALLED and isinstance(seed_data, pd.DataFrame):
            table_headers = list(seed_data.columns)
            # By using `to_json()` we serialize out any Pandas specific data types
            # to scalar values first
            table_data = json.loads(seed_data.to_json(orient="records"))
        else:
            raise GretelInferenceAPIError(
                "Seed data must be a `pandas` DataFrame or a list of dicts."
            )

        total_record_count = len(table_data)

        # Replace our list of records to edit with a list of lists
        # Each inner list will get its own upstream API call
        table_data_chunks: list[list[dict]] = [
            table_data[i : i + chunk_size]
            for i in range(0, total_record_count, chunk_size)
        ]
        table_data = []  # GC it, we don't need it anymore

        def _build_edit_iterator() -> Iterator[dict]:
            for chunk in table_data_chunks:
                ref_data = {
                    "data": {
                        "table_headers": table_headers,
                        "table_data": chunk,
                    }
                }

                stream_iterator = self._stream(
                    prompt=prompt,
                    num_records=len(chunk),
                    params={
                        "temperature": temperature,
                        "top_k": top_k,
                        "top_p": top_p,
                    },
                    ref_data=ref_data,
                )

                for record in stream_iterator:
                    yield record

        # because we process the seed data in chunks,
        # each chunk will get its own upstream request
        # and consequently its own iterator so we wrap
        # each of these calls in one big iterator
        data_iterator = _build_edit_iterator()

        return self._get_stream_results(
            stream_iterator=data_iterator,
            num_records=total_record_count,
            is_streaming=stream,
            as_dataframe=as_dataframe,
            pbar_desc="Editing records",
            disable_pbar=disable_progress_bar,
        )

    def generate(
        self,
        prompt: str,
        *,
        num_records: int,
        temperature: float = TabLLMDefaultParams.temperature,
        top_k: int = TabLLMDefaultParams.top_k,
        top_p: float = TabLLMDefaultParams.top_p,
        stream: bool = False,
        as_dataframe: bool = True,
        disable_progress_bar: bool = False,
        sample_buffer_size: int = 0,
    ) -> StreamReturnType:
        """Generate synthetic data.

        Each request to Gretel will generate at most 50 records at a time. This
        method will make multiple requests, as needed, to fulfill the
        desired `num_records` provided. If a request does not produce
        records within the `self.request_timeout_sec` limit, that request
        will be dropped and new requests will be made automatically.

        When multiple requests are made to fulfill the `num_records` provided
        you may optionally pass the "last N" records generated to subsequent
        requests by setting `sample_buffer_size` to something like 5.
        When sample records are passed to subsequent requests the
        LLM will use that as context for record generation. This is useful
        for keeping continuity in fields between requests (such as monotonic values, etc).



        Args:
            prompt: The prompt for generating synthetic tabular data.
            num_records: The number of records to generate.
            temperature: Sampling temperature. Higher values make output more random.
            top_k: Number of highest probability tokens to keep for top-k filtering.
            top_p: The cumulative probability cutoff for sampling tokens.
            stream: If True, stream the generated data.
            as_dataframe: If True, return the data as a pandas DataFrame. This
                parameter is ignored if `stream` is True.
            disable_progress_bar: If True, disable progress bar.
                Ignored if `stream` is True.
            sample_buffer_size: How many of the last N generated records that should be provided
                as sample data to subsequent generation requests.


        Returns:
            The stream iterator or the generated data records.

        Example::

            from gretel_client.inference_api.tabular import TabularLLMInferenceAPI

            prompt = (
                "Generate positive and negative reviews for the following products: "
                "red travel mug, leather mary jane heels, unicorn LED night light. "
                "Include columns for the product name, number of stars (1-5), review, and customer id."
            )

            tabllm = TabularLLMInferenceAPI(api_key="prompt")

            df = tabllm.generate(prompt=prompt, num_records=10)
        """
        stream_iterator = self._stream(
            prompt=prompt,
            num_records=num_records,
            params={
                "temperature": temperature,
                "top_k": top_k,
                "top_p": top_p,
            },
            sample_buffer_size=sample_buffer_size,
        )

        return self._get_stream_results(
            stream_iterator=stream_iterator,
            num_records=num_records,
            is_streaming=stream,
            as_dataframe=as_dataframe,
            pbar_desc="Generating records",
            disable_pbar=disable_progress_bar,
        )
