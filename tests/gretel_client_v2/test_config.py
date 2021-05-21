import os
from pathlib import Path
from unittest.mock import patch

import pytest

from gretel_client_v2.config import (
    DEFAULT_GRETEL_ENDPOINT,
    GRETEL_API_KEY,
    _ClientConfig,
    write_config,
    _load_config,
    get_session_config,
    configure_session,
    GretelClientConfigurationError
)
from gretel_client_v2.rest.api.projects_api import ProjectsApi
from gretel_client_v2.projects import tmp_project


def test_does_read_and_write_config(tmpdir):
    config = _ClientConfig(
        endpoint=DEFAULT_GRETEL_ENDPOINT,
        api_key="grtu...",
        default_project_name=None,
    )

    tmp_config_path = Path(tmpdir / "config.json")
    config_path = write_config(config, config_path=tmp_config_path)
    assert config_path == tmp_config_path
    assert _load_config(config_path)


def test_does_set_session_factory():
    with patch.dict(os.environ, {}, clear=True):
        config = _ClientConfig(
            endpoint=DEFAULT_GRETEL_ENDPOINT,
            api_key="grtu...",
            default_project_name=None,
        )
    try:
        assert get_session_config() != config
        configure_session(config)
        assert get_session_config() == config
    finally:
        configure_session(_load_config())


def test_can_get_api_bindings():
    client = get_session_config()
    assert isinstance(client.get_api(ProjectsApi), ProjectsApi)


def test_does_check_project():
    config = _ClientConfig(
        endpoint=DEFAULT_GRETEL_ENDPOINT,
        api_key=os.getenv(GRETEL_API_KEY),
    )

    with pytest.raises(GretelClientConfigurationError):
        config.update_default_project("dsflkj")

    with tmp_project() as p:
        config.update_default_project(p.project_id)
        config = _ClientConfig(
            endpoint=DEFAULT_GRETEL_ENDPOINT,
            api_key=os.getenv(GRETEL_API_KEY),
            default_project_name=p.project_id
        )
