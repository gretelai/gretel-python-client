import logging
import sys
import time

from typing import Any, Iterator, List, Optional, Union

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
        logger.info(f"Backend model: {backend_model}")
        self._reset_stream()

    @property
    def backend_model_list(self) -> List[str]:
        """Returns list of available tabular backend models."""
        return [m for m in self._full_model_list if "tabular" in m]

    @property
    def name(self) -> str:
        """Returns display name for this inference api."""
        return "Gretel tabular LLM"

    def _reset_stream(self) -> None:
        """Reset the stream state."""
        self._curr_stream_id = None
        self._next_iter = None
        self._generated_count = 0

    def _create_stream_if_needed(
        self,
        prompt: str,
        *,
        params: dict,
        num_records: int,
        ref_data: Optional[dict] = None,
    ) -> None:
        """Create a new stream if the stream id is not set.

        Args:
            prompt: The prompt for generating synthetic data.
            params: Additional parameters for the model's generation method.
            num_records: The number of records to generate.
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
            self._next_iter = None

    def _stream(
        self,
        prompt: str,
        *,
        num_records: int,
        params: dict,
        ref_data: Optional[dict] = None,
    ) -> Iterator[dict[str, Any]]:
        """Stream data generation with tabular LLM.

        Args:
            prompt: The prompt for generating synthetic data.
            params: Additional parameters for the model's generation method.
            num_records: The number of records to generate.
            ref_data: Flexible option for passing additional data sources.

        Raises
            GretelInferenceAPIError: If the stream is closed with an error.

        Yields:
            The generated data records.
        """
        self._reset_stream()
        while self._generated_count < num_records:
            self._create_stream_if_needed(
                prompt=prompt,
                params=params,
                num_records=num_records,
                ref_data=ref_data,
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
                continue

            # Iterate over the records in the stream.
            for record in data_list:
                data_type = record["data_type"]
                if data_type == "TabularResponse":
                    row_data = record["data"]["table_data"]
                    for row in row_data:
                        self._generated_count += 1
                        yield row
                    if self._generated_count >= num_records:
                        break
                elif data_type == "logger.error":
                    raise GretelInferenceAPIError(record["data"])

            # The stream is exhausted when the state is closed and there's no more data.
            if resp.get("stream_state", {}).get("status") == "closed" and not data_list:
                self._curr_stream_id = None
            else:
                self._next_iter = resp.get("next_iterator")

            time.sleep(STREAM_SLEEP_TIME)

    def _select_stream_results(
        self, is_streaming, stream_iterator, as_dataframe
    ) -> StreamReturnType:
        """Select and return the results for the stream.

        Args:
            is_streaming: If True, return the stream iterator.
            stream_iterator: The stream iterator.
            as_dataframe: If True, return the data as a pandas DataFrame.

        Returns:
            The stream iterator or the generated data records.
        """
        if is_streaming:
            return stream_iterator
        elif PANDAS_IS_INSTALLED and as_dataframe:
            return pd.DataFrame(list(stream_iterator))
        else:
            if as_dataframe:
                logger.error(
                    "Pandas is required to return dataframes. "
                    "Install using `pip install pandas`."
                )
            return list(stream_iterator)

    def display_dataframe_in_notebook(self, dataframe: _DataFrameT) -> None:
        """Display DataFrame in notebook with text wrapping for better visibility.

        Pandas must be installed to run this method.

        Args:
            dataframe: The pandas DataFrame to display.
        """
        if not PANDAS_IS_INSTALLED:
            logger.error(
                "Pandas is required to display dataframes in notebooks. "
                "Install using `pip install pandas`."
            )
            return None

        from IPython.display import display

        display(
            dataframe.style.set_properties(
                **{"text-align": "left", "white-space": "normal", "height": "auto"}
            )
        )

    def edit(
        self,
        prompt: str,
        *,
        seed_data: Union[_DataFrameT, List[dict[str, Any]]],
        temperature: float = TabLLMDefaultParams.temperature,
        top_k: int = TabLLMDefaultParams.top_k,
        top_p: float = TabLLMDefaultParams.top_p,
        stream: bool = False,
        as_dataframe: bool = True,
    ) -> StreamReturnType:
        """Edit the seed data according to the given prompt.

        Args:
            prompt: A prompt specifying how to edit the seed data.
            seed_data: The seed data to edit.
            temperature: Sampling temperature. Higher values make output more random.
            top_k: Number of highest probability tokens to keep for top-k filtering.
            top_p: The cumulative probability cutoff for sampling tokens.
            stream: If True, stream the generated data.
            as_dataframe: If True, return the data as a pandas DataFrame. This
                parameter is ignored if `stream` is True.

        Raises:
            GretelInferenceAPIError: If the seed data is an invalid type.

        Returns:
            The stream iterator or the generated data records.
        """
        if isinstance(seed_data, list) and isinstance(seed_data[0], dict):
            table_headers = list(seed_data[0].keys())
            table_data = seed_data
        elif PANDAS_IS_INSTALLED and isinstance(seed_data, pd.DataFrame):
            table_headers = list(seed_data.columns)
            table_data = seed_data.to_dict(orient="records")
        else:
            raise GretelInferenceAPIError(
                "Seed data must be a `pandas` DataFrame or a list of dicts."
            )

        ref_data = {
            "data": {
                "table_headers": table_headers,
                "table_data": table_data,
            }
        }

        stream_iterator = self._stream(
            prompt=prompt,
            num_records=len(seed_data),
            params={
                "temperature": temperature,
                "top_k": top_k,
                "top_p": top_p,
            },
            ref_data=ref_data,
        )

        return self._select_stream_results(stream, stream_iterator, as_dataframe)

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
    ) -> StreamReturnType:
        """Generate synthetic data.

        Args:
            prompt: The prompt for generating synthetic tabular data.
            num_records: The number of records to generate.
            temperature: Sampling temperature. Higher values make output more random.
            top_k: Number of highest probability tokens to keep for top-k filtering.
            top_p: The cumulative probability cutoff for sampling tokens.
            stream: If True, stream the generated data.
            as_dataframe: If True, return the data as a pandas DataFrame. This
                parameter is ignored if `stream` is True.

        Returns:
            The stream iterator or the generated data records.
        """
        stream_iterator = self._stream(
            prompt=prompt,
            num_records=num_records,
            params={
                "temperature": temperature,
                "top_k": top_k,
                "top_p": top_p,
            },
        )

        return self._select_stream_results(stream, stream_iterator, as_dataframe)
