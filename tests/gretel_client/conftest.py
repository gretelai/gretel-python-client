from pathlib import Path
from unittest.mock import MagicMock

import pytest

from gretel_client.config import (
    configure_session,
    DEFAULT_GRETEL_ARTIFACT_ENDPOINT,
    DEFAULT_RUNNER,
)

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
    return "https://api-dev.gretel.cloud"
