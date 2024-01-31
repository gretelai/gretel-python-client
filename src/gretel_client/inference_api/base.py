import json

from abc import ABC, abstractproperty
from enum import Enum
from typing import Any, Dict, Optional

from gretel_client import analysis_utils
from gretel_client.config import ClientConfig, configure_session, get_session_config
from gretel_client.dataframe import _DataFrameT

MODELS_API_PATH = "/v1/inference/models"


class GretelInferenceAPIError(Exception):
    """Raised when an error occurs with the Inference API."""


class InferenceAPIModelType(str, Enum):
    TABULAR_LLM = "tabllm"


class BaseInferenceAPI(ABC):
    """Base class for Gretel Inference API objects."""

    def __init__(self, *, session: Optional[ClientConfig] = None, **session_kwargs):
        if session is None:
            if len(session_kwargs) > 0:
                configure_session(**session_kwargs)
            session = get_session_config()
        elif len(session_kwargs) > 0:
            raise ValueError("cannot specify session arguments when passing a session")

        if session.default_runner != "cloud":
            raise GretelInferenceAPIError(
                "Gretel's Inference API is currently only "
                "available within Gretel Cloud. Your current runner "
                f"is configured to: {session.default_runner}"
            )
        self._api_client = session._get_api_client()
        self.endpoint = session.endpoint
        self._available_backend_models = [
            m for m in self._call_api("get", self.models_api_path).get("models", [])
        ]

    @abstractproperty
    def api_path(self) -> str:
        ...

    @property
    def models_api_path(self) -> str:
        return MODELS_API_PATH

    @property
    def name(self) -> str:
        return self.__class__.__name__

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

    def _call_api(
        self,
        method: str,
        path: str,
        query_params: Optional[dict] = None,
        body: Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> Dict[str, Any]:
        """Make a direct API call to Gretel Cloud.

        Args:
            method: "get", "post", etc
            path: The full request path, any path params must be already included.
            query_params: Optional URL based query parameters
            body: An optional JSON payload to send
            headers: Any custom headers that need to bet set.

        NOTE:
            This function will automatically inject the appropriate API hostname and
            authentication from the Gretel configuration.
        """
        if headers is None:
            headers = {}

        method = method.upper()

        if not path.startswith("/"):
            path = "/" + path

        # Utilize the ApiClient method to inject the proper authentication
        # into our headers, since Gretel only uses header-based auth we don't
        # need to pass any other data into this
        #
        # NOTE: This function does a pointer-like update of ``headers``
        self._api_client.update_params_for_auth(
            headers=headers,
            querys=None,
            auth_settings=self._api_client.configuration.auth_settings(),
            resource_path=None,
            method=None,
            body=None,
        )

        url = self._api_client.configuration.host + path

        response = self._api_client.request(
            method, url, query_params=query_params, body=body, headers=headers
        )

        return json.loads(response.data.decode())
