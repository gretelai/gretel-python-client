import os

import pytest

from gretel_client.factories import GretelFactories
from gretel_client.inference_api.base import InferenceAPIModelType
from gretel_client.inference_api.tabular import TABLLM_DEFAULT_MODEL


@pytest.fixture(scope="module")
def factories():
    return GretelFactories(
        api_key=os.getenv("GRETEL_API_KEY"), endpoint="https://api-dev.gretel.cloud"
    )


def test_factories_tabllm_initialize_inference_api(factories):
    tabllm = factories.initialize_inference_api(
        InferenceAPIModelType.TABULAR_LLM, backend_model=TABLLM_DEFAULT_MODEL
    )
    assert tabllm.backend_model == TABLLM_DEFAULT_MODEL
    assert tabllm.backend_model in tabllm.backend_model_list
