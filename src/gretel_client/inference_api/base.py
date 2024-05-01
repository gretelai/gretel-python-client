import json
import logging
import sys

from abc import ABC, abstractproperty
from enum import Enum
from typing import Any, Dict, List, Optional

from gretel_client.config import ClientConfig, configure_session, get_session_config

MODELS_API_PATH = "/v1/inference/models"

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


class GretelInferenceAPIError(Exception):
    """Raised when an error occurs with the Inference API."""


class InferenceAPIModelType(str, Enum):
    NAVIGATOR = "navigator"
    NATURAL_LANGUAGE = "natural_language"


class BaseInferenceAPI(ABC):
    """Base class for Gretel Inference API objects."""

    _available_backend_models: list[str]
    _model_type: str

    def __init__(
        self,
        backend_model: Optional[str] = None,
        *,
        session: Optional[ClientConfig] = None,
        **session_kwargs,
    ):
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
        self.endpoint = session.endpoint
        self._api_client = session._get_api_client()
        self._available_backend_models = [
            m for m in self._call_api("get", self.models_api_path).get("models", [])
        ]
        self.backend_model = backend_model

    @abstractproperty
    def api_path(self) -> str: ...

    @abstractproperty
    def model_type(self) -> str: ...

    @property
    def backend_model_list(self) -> List[str]:
        """Returns list of backend models for this model type."""
        return [
            m["model_id"]
            for m in self._available_backend_models
            if m["model_type"].upper() == self.model_type.upper()
        ]

    @property
    def backend_model(self) -> str:
        return self._backend_model

    @backend_model.setter
    def backend_model(self, backend_model: str) -> None:
        if backend_model is None:
            backend_model = self.backend_model_list[0]
        elif backend_model not in self.backend_model_list:
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
