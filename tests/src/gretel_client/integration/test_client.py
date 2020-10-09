import os
import pytest

from gretel_client.client import (
    get_cloud_client,
    Client,
)
from gretel_client.errors import NotFound

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


def test_list_samples(client: Client):
    samples = client.list_samples()
    assert len(samples) > 0
    assert all([type(s) == str for s in samples])

    samples = client.list_samples(include_details=True)
    assert all([type(s) == dict for s in samples])


def test_get_samples(client: Client):
    sample = client.get_sample("safecast")
    assert len(sample) > 0

    sample = client.get_sample("safecast", as_df=True)
    assert len(sample) > 0

def test_get_sample_not_found(client: Client):
    with pytest.raises(NotFound):
        client.get_sample("this_sample_not_found")
