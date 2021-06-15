from pathlib import Path
from unittest.mock import MagicMock

import pytest

from gretel_client_v2.config import configure_session

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def get_fixture():
    def _(name: str) -> Path:
        return FIXTURES / name

    return _


@pytest.fixture(scope="function", autouse=True)
def configure_session_client():
    configure_session(MagicMock())


@pytest.fixture
def dev_ep() -> str:
    return "https://api-dev.gretel.cloud"
