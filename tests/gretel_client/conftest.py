from pathlib import Path
from unittest.mock import MagicMock

import pytest

from gretel_client.config import (
    DEFAULT_GRETEL_ARTIFACT_ENDPOINT,
    DEFAULT_RUNNER,
    configure_session,
)
from gretel_client.test_utils import TestGretelApiFactory, TestGretelResourceProvider

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def get_fixture():
    def _(name: str) -> Path:
        return FIXTURES / name

    return _


@pytest.fixture(scope="function", autouse=True)
def configure_session_client():
    configure_session(
        MagicMock(
            default_runner=DEFAULT_RUNNER,
            artifact_endpoint=DEFAULT_GRETEL_ARTIFACT_ENDPOINT,
            tenant_name=None,
        ),
        validate=False,
    )


@pytest.fixture
def dev_ep() -> str:
    return "https://api.dev.gretel.ai"


@pytest.fixture(scope="function")
def api_provider_mock() -> TestGretelApiFactory:
    return TestGretelApiFactory()


@pytest.fixture(scope="function")
def resource_provider_mock() -> TestGretelResourceProvider:
    return TestGretelResourceProvider()
