from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def get_fixture():
    def _(name: str) -> Path:
        return FIXTURES / name
    return _
