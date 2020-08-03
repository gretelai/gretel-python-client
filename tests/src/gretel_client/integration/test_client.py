import os
import pytest

from gretel_client.client import (
    get_cloud_client,
    Client,
)

API_KEY = os.getenv("GRETEL_TEST_API_KEY")


if not API_KEY:
    raise AttributeError("GRETEL_TEST_API_KEY must be set!")


@pytest.fixture(scope="module")
def client():
    return get_cloud_client("api-dev", API_KEY)


def test_detect_entities(client: Client):
    payload = {"email": "test@gretel.ai"}
    detected_entities = client.detect_entities(payload)

    assert len(detected_entities) == 1
    assert len(detected_entities[0]["metadata"]["fields"]["email"]["ner"]["labels"]) == 3
