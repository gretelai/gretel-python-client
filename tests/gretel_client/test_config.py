import os

from pathlib import Path
from unittest.mock import patch

import pytest

from gretel_client.config import (
    _load_config,
    ClientConfig,
    configure_session,
    get_session_config,
    write_config,
)
from gretel_client.rest.api.projects_api import ProjectsApi


def test_does_read_and_write_config(dev_ep, tmpdir):
    config = ClientConfig(
        endpoint=dev_ep,
        api_key="grtu...",
        default_project_name=None,
    )

    tmp_config_path = Path(tmpdir / "config.json")
    config_path = write_config(config, config_path=tmp_config_path)
    assert config_path == tmp_config_path
    assert _load_config(config_path)


def test_does_set_session_factory(dev_ep):
    with patch.dict(os.environ, {}, clear=True):
        config = ClientConfig(
            endpoint=dev_ep,
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
