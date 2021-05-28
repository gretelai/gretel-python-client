import os
from pathlib import Path
from python.src.gretel_client_v2.config import (
    _ClientConfig,
    _load_config,
    configure_session,
)
from unittest.mock import patch

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def get_fixture():
    def _(name: str) -> Path:
        return FIXTURES / name

    return _


@pytest.fixture(scope="function", autouse=True)
def configure_session_client():
    """Ensures the the host client config is reset after each test."""
    with patch.dict(
        os.environ, {"GRETEL_API_KEY": os.getenv("GRETEL_API_KEY")}, clear=True
    ):
        configure_session(_ClientConfig())
    yield
    configure_session(_load_config())
