import json
import os
import platform

from pathlib import Path
from typing import Callable, Dict
from unittest.mock import patch

import pytest

from click.testing import CliRunner, Result

from gretel_client._hybrid.config import configure_hybrid_session
from gretel_client.config import (
    _load_config,
    ClientConfig,
    configure_session,
    get_session_config,
)
from gretel_client.projects.models import Model
from gretel_client.projects.projects import get_project, Project
from gretel_client.rest.api.projects_api import ProjectsApi
from gretel_client.rest_v1.api.connections_api import ConnectionsApi

fixtures = Path(__file__).parent / "../fixtures"


@pytest.fixture
def get_fixture():
    def _(name: str) -> Path:
        return fixtures / name

    return _


@pytest.fixture
def runner() -> CliRunner:
    """Returns a CliRunner that can be used to invoke the CLI from
    unit tests.
    """
    return CliRunner(mix_stderr=False)


@pytest.fixture(autouse=True)
def configure_session_client(request):
    """Ensures the the host client config is reset after each test."""
    gretel_api_key = os.getenv("GRETEL_API_KEY")
    assert gretel_api_key is not None, "GRETEL_API_KEY must be set!"
    with patch.dict(
        os.environ,
        {
            "GRETEL_API_KEY": os.getenv("GRETEL_API_KEY"),
            "GRETEL_ENDPOINT": "https://api-dev.gretel.cloud",
        },
        clear=True,
    ):
        config = ClientConfig.from_env()
        configurator = configure_session
        configure_kwargs = {}

        hybrid = request.node.get_closest_marker("gretel_hybrid")
        if hybrid:
            configurator = configure_hybrid_session
            configure_kwargs = hybrid.kwargs or {}

        configurator(config, **configure_kwargs)

    yield
    configure_session(_load_config())


@pytest.fixture
def project():
    p = get_project(create=True)
    yield p
    p.delete()


@pytest.fixture
def projects_api() -> ProjectsApi:
    return get_session_config().get_api(ProjectsApi)


@pytest.fixture
def connections_api() -> ConnectionsApi:
    return get_session_config().get_v1_api(ConnectionsApi)


@pytest.fixture
def pretrained_model_map(get_fixture: Callable) -> Dict[str, str]:
    return json.loads(get_fixture("model_fixtures.json").read_text())


@pytest.fixture
def pretrained_project(pretrained_model_map: Dict) -> Project:
    return get_project(
        name=os.getenv("GRETEL_PROJECT", pretrained_model_map["_project"])
    )


@pytest.fixture
def get_pretrained_model(pretrained_project: Project, pretrained_model_map: Dict):
    def _(name: str) -> Model:
        return pretrained_project.get_model(model_id=pretrained_model_map[name])

    return _


@pytest.fixture
def trained_synth_model(get_pretrained_model: Callable) -> Model:
    return get_pretrained_model("synthetics_default")


@pytest.fixture
def trained_xf_model(get_pretrained_model: Callable) -> Model:
    return get_pretrained_model("transforms_default")


@pytest.fixture
def trained_classify_model(get_pretrained_model: Callable) -> Model:
    return get_pretrained_model("classify_default")


def print_cmd_output(cmd: Result):
    print(f"STDERR\n{cmd.stderr}")
    print(f"STDOUT\n{cmd.stdout}")


pytest_skip_on_windows = pytest.mark.skipif(
    platform.system() == "Windows", reason="Skip local runner test for Windows"
)
