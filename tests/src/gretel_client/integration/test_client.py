import os
import pytest

import pandas as pd

from gretel_client.client import (
    get_cloud_client,
    Client,
    temporary_project,
)
from gretel_client.errors import NotFound, BadRequest

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
    assert (
        len(detected_entities[0]["metadata"]["fields"]["email"]["ner"]["labels"]) == 3
    )


def test_list_samples(client: Client):
    samples = client.list_samples()
    assert len(samples) > 0
    assert all([type(s) == str for s in samples])

    samples = client.list_samples(include_details=True)
    assert all([type(s) == dict for s in samples])

    # check to make sure query params propagate to the request
    with pytest.raises(BadRequest):
        client.list_samples(params={"bad_param": "yes"})


def test_get_samples(client: Client):
    sample = client.get_sample("safecast")
    assert len(sample) > 0

    sample = client.get_sample("safecast", as_df=True, params={"limit": 10})
    assert isinstance(sample, pd.DataFrame)
    assert len(sample) == 10


def test_get_sample_not_found(client: Client):
    with pytest.raises(NotFound):
        client.get_sample("this_sample_not_found")


def test_bulk_record_summary_count(client: Client):
    samples = client.get_sample("safecast", params={"limit": 150})
    with temporary_project(client) as project:
        summary = project.send_bulk(samples)
        assert summary.records_sent == len(samples)
