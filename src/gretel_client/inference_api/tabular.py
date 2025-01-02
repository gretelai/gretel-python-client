import copy
import json
import logging
import sys
import time

from typing import Any, Iterator, List, Optional, Union

from tqdm import tqdm

from gretel_client import analysis_utils
from gretel_client.dataframe import _DataFrameT
from gretel_client.gretel.artifact_fetching import PANDAS_IS_INSTALLED
from gretel_client.gretel.config_setup import NavigatorDefaultParams
from gretel_client.inference_api.base import BaseInferenceAPI, GretelInferenceAPIError

if PANDAS_IS_INSTALLED:
    import pandas as pd

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

STREAM_SLEEP_TIME = 0.5
MAX_ROWS_PER_STREAM = 100
REQUEST_TIMEOUT_SEC = 60
TABULAR_API_PATH = "/v1/inference/tabular/"
PROGRESS_BAR_FORMAT = "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed},{rate_noinv_fmt}]"

StreamReturnType = Union[
    Iterator[dict[str, Any]], _DataFrameT, List[dict[str, Any]], dict[str, Any]
]


class TabularInferenceAPI(BaseInferenceAPI):
    """Inference API for real-time data generation with Gretel Navigator.

    Args:
        backend_model (str, optional): The model that is used under the hood.
            If None, the latest default model will be used. See the
            `backend_model_list` property for a list of available models.
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

    max_retry_count: int = 3
    """
    How many errors or timeouts should be tolerated before the generation
    process raises a user-facing error.
    """

    @property
    def api_path(self) -> str:
        return TABULAR_API_PATH

    @property
    def model_type(self) -> str:
        return "tabular"

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
    def name(self) -> str:
        """Returns display name for this inference api."""
        return "Navigator Tabular"

    def display_dataframe_in_notebook(
        self, dataframe: _DataFrameT, settings: Optional[dict] = None
    ) -> None:
        """Display pandas DataFrame in notebook with better settings for readability.

        This function is intended to be used in a Jupyter notebook.

        Args:
            dataframe: The pandas DataFrame to display.
            settings: Optional properties to set on the DataFrame's style.
                If None, default settings with text wrapping are used.
        """
        analysis_utils.display_dataframe_in_notebook(dataframe, settings)

    def _reset_stream(self) -> None:
        """Reset the stream state."""
        self._curr_stream_id = None
        self._last_stream_read = None
        self._next_iter = None
        self._generated_count = 0
        self._set_response_metadata({})

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

        attempt_count = 0
        response_metadata: dict = {}

        # Keep going until we've given the user all their records and the most recent
        # stream has been closed. We need to ensure the last stream is closed because
        # that's when we receive additional metadata (model_ids, billing, etc).
        while self._generated_count < num_records or self._curr_stream_id:
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
                elif record["data_type"] == "ResponseMetadata":
                    response_metadata = _combine_response_metadata(
                        response_metadata, json.loads(record["data"])
                    )
                    self._set_response_metadata(response_metadata)
                elif record["data_type"] == "TabularResponse":
                    row_data = record["data"]["table_data"]
                    for row in row_data:
                        self._generated_count += 1
                        self._last_stream_read = time.time()
                        if self._generated_count > num_records:
                            # If we've already sent the user back all of their requested results,
                            # don't bother returning more data than they expect.
                            continue
                        if sample_buffer_size > 0:
                            history_buffer.append(row)
                            history_buffer = history_buffer[-sample_buffer_size:]
                        yield row
                elif record["data_type"] == "logger.error":
                    attempt_count += 1
                    err_string = record["data"]
                    if attempt_count >= self.max_retry_count:
                        raise GretelInferenceAPIError(
                            f"Failed to generate data after {attempt_count} attempts, error from API: {err_string}"
                        )
                    else:
                        logger.warning(
                            "Received error during generation: '%s'. Retrying.",
                            err_string,
                        )
                        self._curr_stream_id = None

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
                attempt_count += 1
                if attempt_count >= self.max_retry_count:
                    raise GretelInferenceAPIError(
                        f"Failed to generate data after {attempt_count} attempts."
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
            total=num_records,
            desc=pbar_desc,
            disable=disable_pbar,
            unit=" records",
            bar_format=PROGRESS_BAR_FORMAT,
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
        temperature: float = NavigatorDefaultParams.temperature,
        top_k: int = NavigatorDefaultParams.top_k,
        top_p: float = NavigatorDefaultParams.top_p,
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

            from gretel_client.inference_api.tabular import TabularInferenceAPI

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

            tabular = TabularInferenceAPI(api_key="prompt")

            df = tabular.edit(prompt=prompt, seed_data=seed_data)
        """
        table_headers, table_data = _data_input_to_api_data(seed_data, "Seed data")
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
        temperature: float = NavigatorDefaultParams.temperature,
        top_k: int = NavigatorDefaultParams.top_k,
        top_p: float = NavigatorDefaultParams.top_p,
        sample_data: Optional[Union[_DataFrameT, List[dict[str, Any]]]] = None,
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
            sample_data: The sample data to guide the initial generation process.
                Things to keep in mind when using this parameter:
                - The generated data will use exact column names from "sample_data".
                - It's important for the prompt and "sample_data" match.
                  For example, they should refer to the same columns.
                - Use sample data to provide examples of the data you want generated,
                  E.g. if you need specific data formats.
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

            from gretel_client.inference_api.tabular import TabularInferenceAPI

            prompt = (
                "Generate positive and negative reviews for the following products: "
                "red travel mug, leather mary jane heels, unicorn LED night light. "
                "Include columns for the product name, number of stars (1-5), review, and customer id."
            )

            tabular = TabularInferenceAPI(api_key="prompt")

            df = tabular.generate(prompt=prompt, num_records=10)

            # Another example with sample data
            sample_data = [
                {
                    "review_date": "2021-01-01",
                    "product_name": "red travel mug",
                    "stars": 5,
                    "review": "I love this mug!",
                    "customer_id": 123,
                }
            ]
            df = tabular.generate(prompt=prompt, num_records=10, sample_data=sample_data)
        """
        ref_data = {}
        if sample_data:
            table_headers, table_data = _data_input_to_api_data(
                sample_data, "Sample data"
            )
            ref_data = {
                "sample_data": {
                    "table_headers": table_headers,
                    "table_data": table_data,
                }
            }

        stream_iterator = self._stream(
            prompt=prompt,
            num_records=num_records,
            params={
                "temperature": temperature,
                "top_k": top_k,
                "top_p": top_p,
            },
            sample_buffer_size=sample_buffer_size,
            ref_data=ref_data,
        )

        return self._get_stream_results(
            stream_iterator=stream_iterator,
            num_records=num_records,
            is_streaming=stream,
            as_dataframe=as_dataframe,
            pbar_desc="Generating records",
            disable_pbar=disable_progress_bar,
        )


def _data_input_to_api_data(
    data: Union[_DataFrameT, List[dict[str, Any]]], data_type: str
) -> tuple[list[str], list[dict[str, Any]]]:
    """
    Converts data input from the high level SDK to the format that our API expects:
    - table_headers: contains list of columns
    - table_data: contains list of records

    Raises:
        GretelInferenceAPIError: If the data input is invalid.
    """
    if isinstance(data, list) and isinstance(data[0], dict):
        table_headers = list(data[0].keys())
        table_data = data
    elif PANDAS_IS_INSTALLED and isinstance(data, pd.DataFrame):
        table_headers = list(data.columns)
        # By using `to_json()` we serialize out any Pandas specific data types
        # to scalar values first
        table_data = json.loads(data.to_json(orient="records"))
    else:
        raise GretelInferenceAPIError(
            f"{data_type} must be a `pandas` DataFrame or a list of dicts."
        )

    return table_headers, table_data


def _combine_response_metadata(
    response_metadata: dict, new_response_data: dict
) -> dict:
    if not response_metadata:
        return new_response_data

    for k, v in new_response_data["usage"].items():
        if not isinstance(v, int):
            continue
        response_metadata["usage"][k] += v

    return response_metadata
