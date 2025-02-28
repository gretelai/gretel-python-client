import json
import logging
import sys

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional

from gretel_client.config import ClientConfig, configure_session, get_session_config
from gretel_client.rest.api_client import ApiClient

MODELS_API_PATH = "/v1/inference/models"

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


class GretelInferenceAPIError(Exception):
    """Raised when an error occurs with the Inference API."""


class InferenceAPIModelType(str, Enum):
    TABULAR = "tabular"
    NATURAL_LANGUAGE = "natural_language"


def call_api(
    api_client: ApiClient,
    method: str,
    path: str,
    query_params: Optional[dict] = None,
    body: Optional[dict] = None,
    headers: Optional[dict] = None,
) -> Dict[str, Any]:
    """Make a direct API call to Gretel Cloud.

    Args:
        api_client: The Gretel API client
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
    api_client.update_params_for_auth(
        headers=headers,
        querys=None,
        auth_settings=api_client.configuration.auth_settings(),
        resource_path=None,
        method=None,
        body=None,
    )

    url = api_client.configuration.host + path

    response = api_client.request(
        method, url, query_params=query_params, body=body, headers=headers
    )

    return json.loads(response.data.decode())


def get_full_navigator_model_list(api_client: ApiClient) -> List[Dict[str, str]]:
    """Returns a list of dicts with info on all available Navigator backend models.

    Args:
        api_client: The Gretel API client

    Returns:
        List of dicts of all available backend models.
    """

    available_models = call_api(api_client, "get", MODELS_API_PATH)

    if (
        available_models is None
        or "models" not in available_models
        or len(available_models["models"]) == 0
    ):
        raise GretelInferenceAPIError(
            f"API call to {MODELS_API_PATH} failed to retrieve list of "
            "Navigator backend models."
        )

    available_models = available_models["models"]

    if not all(k in m for m in available_models for k in ["model_id", "model_type"]):
        raise GretelInferenceAPIError(
            "Navigator model IDs and/or types not returned with API call "
            f"to {MODELS_API_PATH}."
        )

    return available_models


def get_model(
    api_client: ApiClient, model_id: str = None, model_type: str = None
) -> Dict[str, str]:
    """Returns a Navigator backend model by model_id and model_type.

    Args:
        api_client: The Gretel API client
        model_id: Model ID
        model_type: Type of the models (natural, tabular)

    Returns:
        Backend model.
    """

    available_model = call_api(
        api_client,
        "get",
        MODELS_API_PATH,
        query_params={"model_id": model_id, "model_type": model_type},
    )
    model = available_model["models"][0] if available_model["models"] else None

    return model


class BaseInferenceAPI(ABC):
    """Base class for Gretel Inference API objects."""

    _available_backend_models: list[str]
    _model_type: str
    _response_metadata: dict

    def __init__(
        self,
        backend_model: Optional[str] = None,
        *,
        verify_ssl: bool = True,
        session: Optional[ClientConfig] = None,
        skip_configure_session: Optional[bool] = False,
        **session_kwargs,
    ):
        if session is None:
            # Only used for unit tests
            if not skip_configure_session:
                configure_session(**session_kwargs)
            session = get_session_config()
        elif len(session_kwargs) > 0:
            raise ValueError("cannot specify session arguments when passing a session")

        if session.default_runner != "cloud" and not ".serverless." in session.endpoint:
            raise GretelInferenceAPIError(
                "Gretel's Inference API is currently only "
                "available within Gretel Cloud. Your current runner "
                f"is configured to: {session.default_runner}"
            )
        self.endpoint = session.endpoint
        self._api_client = session._get_api_client(verify_ssl=verify_ssl)
        self._available_backend_models = get_full_navigator_model_list(self._api_client)
        self._response_metadata = {}
        self.backend_model = backend_model

    @property
    @abstractmethod
    def api_path(self) -> str: ...

    @property
    @abstractmethod
    def model_type(self) -> str: ...

    @property
    def backend_model_list(self) -> List[str]:
        """Returns list of backend models for this model type."""
        return [
            m["model_id"]
            for m in self._available_backend_models
            if m["model_type"].casefold() == self.model_type.casefold()
        ]

    @property
    def backend_model(self) -> str:
        return self._backend_model

    @backend_model.setter
    def backend_model(self, backend_model: str) -> None:
        if backend_model is None:
            backend_model = self.backend_model_list[0]
        elif backend_model not in self.backend_model_list:
            model = get_model(
                self._api_client, model_id=backend_model, model_type=self.model_type
            )
            if model is None or backend_model != model.get("model_id"):
                raise GretelInferenceAPIError(
                    f"Model {backend_model} is not a valid backend model. "
                    f"Valid models are: {self.backend_model_list}"
                )
        self._backend_model = backend_model
        logger.info("Backend model: %s", backend_model)

    @property
    def models_api_path(self) -> str:
        return MODELS_API_PATH

    @property
    def name(self) -> str:
        return self.__class__.__name__

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
        """
        return call_api(
            api_client=self._api_client,
            method=method,
            path=path,
            query_params=query_params,
            body=body,
            headers=headers,
        )

    def _set_response_metadata(self, response_metadata: dict) -> None:
        self._response_metadata = response_metadata

    def get_response_metadata(self) -> dict:
        if not self._response_metadata:
            raise GretelInferenceAPIError(
                "Response metadata is only set after a request has completed."
            )
        return self._response_metadata
