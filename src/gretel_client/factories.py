import logging
import sys

from typing import Optional

from gretel_client.config import ClientConfig, configure_session, get_session_config
from gretel_client.inference_api.base import (
    BaseInferenceAPI,
    GretelInferenceAPIError,
    InferenceAPIModelType,
)
from gretel_client.inference_api.tabular import (
    TABLLM_DEFAULT_MODEL,
    TabularLLMInferenceAPI,
)

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


class GretelFactories:
    """A class for creating objects that interact with Gretel's APIs."""

    _session: ClientConfig

    def __init__(self, *, session: Optional[ClientConfig] = None, **session_kwargs):
        if session is None:
            if len(session_kwargs) > 0:
                configure_session(**session_kwargs)
            session = get_session_config()
        elif len(session_kwargs) > 0:
            raise ValueError("cannot specify session arguments when passing a session")
        self._session = session

    def initialize_inference_api(
        self,
        model_type: InferenceAPIModelType = InferenceAPIModelType.TABULAR_LLM,
        *,
        backend_model: Optional[str] = None,
    ) -> BaseInferenceAPI:
        """Initializes and returns a gretel inference API object.

        Args:
            model_type: The type of the inference API model.
            backend_model: The model used under the hood by the inference API.

        Raises:
            GretelInferenceAPIError: If the specified model type is not valid.

        Returns:
            An instance of the initialized inference API object.
        """
        if model_type == InferenceAPIModelType.TABULAR_LLM:
            gretel_api = TabularLLMInferenceAPI(
                backend_model=backend_model or TABLLM_DEFAULT_MODEL,
                session=self._session,
            )
        else:
            raise GretelInferenceAPIError(
                f"{model_type} is not a valid inference API model type."
                f"Valid types are {[t.value for t in InferenceAPIModelType]}"
            )
        logger.info("API path: %s%s", gretel_api.endpoint, gretel_api.api_path)
        logger.info("Initialized %s 🚀", gretel_api.name)
        return gretel_api
