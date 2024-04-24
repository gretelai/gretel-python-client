import os

import pytest

from gretel_client.factories import GretelFactories
from gretel_client.inference_api.base import InferenceAPIModelType
from gretel_client.inference_api.tabular import NAVIGATOR_DEFAULT_MODEL


@pytest.fixture(scope="module")
def factories():
    return GretelFactories(
        api_key=os.getenv("GRETEL_API_KEY"), endpoint="https://api-dev.gretel.cloud"
    )


def test_factories_navigator_initialize_inference_api(factories):
    nav = factories.initialize_inference_api(
        InferenceAPIModelType.NAVIGATOR, backend_model=NAVIGATOR_DEFAULT_MODEL
    )
    assert nav.backend_model == NAVIGATOR_DEFAULT_MODEL
    assert nav.backend_model in nav.backend_model_list
