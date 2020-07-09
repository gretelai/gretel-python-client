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

    expected = {
        "email": {
            "ner": {
                "labels": [
                    {
                        "text": "gretel.ai",
                        "start": 5,
                        "end": 14,
                        "label": "domain_name",
                        "source": "regex_domain_name",
                        "score": 0.2,
                    },
                    {
                        "text": "test@gretel.ai",
                        "start": 0,
                        "end": 14,
                        "label": "email_address",
                        "source": "regex_email",
                        "score": 0.8,
                    },
                ]
            }
        }
    }

    assert len(detected_entities) == 1
    assert detected_entities[0]["metadata"]["fields"] == expected
