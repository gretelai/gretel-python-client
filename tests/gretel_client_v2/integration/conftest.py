import os
from unittest.mock import patch

import pytest

from gretel_client_v2.config import (
    ClientConfig,
    _load_config,
    configure_session,
    get_session_config,
)
from gretel_client_v2.projects.projects import get_project, Project
from gretel_client_v2.projects.models import Model
from gretel_client_v2.rest.api.projects_api import ProjectsApi


@pytest.fixture(scope="function", autouse=True)
def configure_session_client(configure_session_client):
    """Ensures the the host client config is reset after each test."""
    with patch.dict(
        os.environ,
        {
            "GRETEL_API_KEY": os.getenv("GRETEL_API_KEY"),
            "GRETEL_ENDPOINT": "https://api-dev.gretel.cloud",
        },
        clear=True,
    ):
        configure_session(ClientConfig.from_env())
    yield
    configure_session(_load_config())


@pytest.fixture
def project(request):
    p = get_project(create=True)
    request.addfinalizer(p.delete)
    return p


@pytest.fixture
def projects_api() -> ProjectsApi:
    return get_session_config().get_api(ProjectsApi)


@pytest.fixture
def pre_trained_project() -> Project:
    yield get_project(name="gretel-client-project-pretrained")


@pytest.fixture
def trained_synth_model(pre_trained_project: Project) -> Model:
    return pre_trained_project.get_model(model_id="60b8f345e00f682f45819019")


@pytest.fixture
def trained_xf_model(pre_trained_project: Project) -> Model:
    return pre_trained_project.get_model(model_id="60b8f50ae00f682f4581901d")
