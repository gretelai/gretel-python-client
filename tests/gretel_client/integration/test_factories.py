import os

import pytest

from gretel_client.factories import GretelFactories
from gretel_client.inference_api.base import InferenceAPIModelType


@pytest.fixture(scope="module")
def factories():
    return GretelFactories(
        api_key=os.getenv("GRETEL_API_KEY"), endpoint="https://api-dev.gretel.cloud"
    )


def test_factories_navigator_initialize_inference_api(factories):
    nav = factories.initialize_inference_api(InferenceAPIModelType.NAVIGATOR)
    assert nav.backend_model == nav.backend_model_list[0]


def test_factories_natural_language_initialize_inference_api(factories):
    llm = factories.initialize_inference_api(InferenceAPIModelType.NATURAL_LANGUAGE)
    assert llm.backend_model == llm.backend_model_list[0]
