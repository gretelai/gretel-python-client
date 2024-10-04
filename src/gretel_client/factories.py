import logging
import sys

from typing import Optional

from gretel_client.config import ClientConfig, configure_session, get_session_config
from gretel_client.inference_api.base import (
    BaseInferenceAPI,
    get_full_navigator_model_list,
    GretelInferenceAPIError,
    InferenceAPIModelType,
)
from gretel_client.inference_api.natural_language import NaturalLanguageInferenceAPI
from gretel_client.inference_api.tabular import TabularInferenceAPI

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


class GretelFactories:
    """A class for creating objects that interact with Gretel's APIs."""

    _session: ClientConfig

    def __init__(
        self,
        *,
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
        self._session = session

    def get_navigator_model_list(self, model_type: InferenceAPIModelType) -> list[str]:
        """Returns a list of available backend models for the specified model type.

        Args:
            model_type: The type of the inference API model.

        Raises:
            GretelInferenceAPIError: If the specified model type is not valid.

        Returns:
            A list of available backend models for the specified model type.
        """
        try:
            model_type = InferenceAPIModelType(model_type).value
        except ValueError:
            raise GretelInferenceAPIError(
                f"{model_type} is not a valid inference API model type. "
                f"Valid types are {[t.value for t in InferenceAPIModelType]}"
            )
        model_type = "natural" if model_type == "natural_language" else model_type
        return [
            m["model_id"]
            for m in get_full_navigator_model_list(self._session._get_api_client())
            if m["model_type"].casefold() == model_type.casefold()
        ]

    def initialize_navigator_api(
        self,
        model_type: InferenceAPIModelType = InferenceAPIModelType.TABULAR,
        backend_model: Optional[str] = None,
    ) -> BaseInferenceAPI:
        """Initializes and returns a gretel inference API object.

        Args:
            model_type: The type of the inference API model.
            backend_model: The model used under the hood by the inference API.
                If None, the latest default model will be used.

        Raises:
            GretelInferenceAPIError: If the specified model type is not valid.

        Returns:
            An instance of the initialized inference API object.
        """

        if model_type == InferenceAPIModelType.TABULAR:
            inference_api_cls = TabularInferenceAPI
        elif model_type == InferenceAPIModelType.NATURAL_LANGUAGE:
            inference_api_cls = NaturalLanguageInferenceAPI
        else:
            raise GretelInferenceAPIError(
                f"{model_type} is not a valid inference API model type."
                f"Valid types are {[t.value for t in InferenceAPIModelType]}"
            )
        gretel_api = inference_api_cls(
            backend_model=backend_model, session=self._session
        )
        logger.info("API path: %s%s", gretel_api.endpoint, gretel_api.api_path)
        logger.info("%s initialized 🚀", gretel_api.name)
        return gretel_api
