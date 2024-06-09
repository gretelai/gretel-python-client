import logging
import sys

from gretel_client.gretel.config_setup import NaturalLanguageDefaultParams
from gretel_client.inference_api.base import BaseInferenceAPI

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

NATURAL_LANGUAGE_API_PATH = "/v1/inference/natural/"


class NaturalLanguageInferenceAPI(BaseInferenceAPI):
    """Inference API for real-time text generation with Gretel Natural Language.

    Args:
        backend_model (str, optional): The model that is used under the hood.
            If None, the latest default model will be used. See the
            `backend_model_list` property for a list of available models.
        **session_kwargs: kwargs for your Gretel session.

    Raises:
        GretelInferenceAPIError: If the specified backend model is not valid.
    """

    @property
    def api_path(self) -> str:
        return NATURAL_LANGUAGE_API_PATH

    @property
    def model_type(self) -> str:
        return "natural"

    @property
    def generate_api_path(self) -> str:
        return self.api_path + "generate"

    @property
    def name(self) -> str:
        """Returns display name for this inference api."""
        return "Navigator LLM"

    def generate(
        self,
        prompt: str,
        temperature: float = NaturalLanguageDefaultParams.temperature,
        max_tokens: int = NaturalLanguageDefaultParams.max_tokens,
        top_p: float = NaturalLanguageDefaultParams.top_p,
        top_k: int = NaturalLanguageDefaultParams.top_k,
    ):
        """Generate synthetic text.

        Args:
            prompt: The prompt for generating synthetic tabular data.
            temperature: Sampling temperature. Higher values make output more random.
            max_tokens: The maximum number of tokens to generate.
            top_k: Number of highest probability tokens to keep for top-k filtering.
            top_p: The cumulative probability cutoff for sampling tokens.

        Example::

            from gretel_client.inference_api.natural_language import NaturalLanguageInferenceAPI

            llm = NaturalLanguageInferenceAPI(api_key="prompt")

            prompt = "Tell me a funny joke about data scientists."

            text = llm.generate(prompt=prompt, temperature=0.5, max_tokens=100)

        Returns:
            The generated text as a string.
        """
        response = self._call_api(
            method="post",
            path=self.generate_api_path,
            body={
                "model_id": self.backend_model,
                "prompt": prompt,
                "params": {
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                    "top_p": top_p,
                    "top_k": top_k,
                },
            },
        )
        return response["text"]
