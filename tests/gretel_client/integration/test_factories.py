import os

import pytest

from gretel_client.factories import GretelFactories
from gretel_client.inference_api.base import (
    GretelInferenceAPIError,
    InferenceAPIModelType,
)


@pytest.fixture(scope="module")
def factories():
    return GretelFactories(
        api_key=os.getenv("GRETEL_API_KEY"), endpoint="https://api-dev.gretel.cloud"
    )


def test_factories_navigator_model_list(factories):
    tab_models = factories.get_navigator_model_list(InferenceAPIModelType.TABULAR)
    assert len(tab_models) > 0

    llm_models = factories.get_navigator_model_list(
        InferenceAPIModelType.NATURAL_LANGUAGE
    )
    assert len(llm_models) > 0


def test_factories_navigator_initialize_inference_api(factories):
    tab = factories.initialize_navigator_api(InferenceAPIModelType.TABULAR)
    assert tab.backend_model == tab.backend_model_list[0]


def test_factories_natural_language_initialize_inference_api(factories):
    llm = factories.initialize_navigator_api(InferenceAPIModelType.NATURAL_LANGUAGE)
    assert llm.backend_model == llm.backend_model_list[0]


def test_factories_get_navigator_model_list_invalid_model_type(factories):
    with pytest.raises(GretelInferenceAPIError):
        factories.get_navigator_model_list("invalid_model_type")
