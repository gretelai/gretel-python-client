import os

from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch

import certifi
import pytest

from gretel_client.config import (
    _load_config,
    ClientConfig,
    configure_session,
    get_session_config,
    GRETEL_PREVIEW_FEATURES,
    PreviewFeatures,
    write_config,
)
from gretel_client.rest.api.projects_api import ProjectsApi


def test_does_read_and_write_config(dev_ep, tmpdir):
    config = ClientConfig(
        endpoint=dev_ep,
        api_key="grtu...",
        default_project_name=None,
        preview_features=PreviewFeatures.ENABLED.value,
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


@patch.dict(os.environ, {"GRETEL_API_KEY": "grtutest"})
def test_can_get_api_bindings():
    client = get_session_config()
    assert isinstance(client.get_api(ProjectsApi), ProjectsApi)


def test_configure_preview_features():
    with patch.dict(
        os.environ,
        {GRETEL_PREVIEW_FEATURES: PreviewFeatures.ENABLED.value},
    ):
        configure_session(
            _load_config()
        )  # ensure the session is reloaded with new env variables
        assert get_session_config().preview_features_enabled


@patch("gretel_client.config.get_session_config")
@patch("gretel_client.config._get_config_path")
@patch("gretel_client.config.getpass")
@patch("gretel_client.config.write_config")
def test_configure_session_with_cache(
    write_config: MagicMock,
    get_pass: MagicMock,
    _get_config_path: MagicMock,
    get_session_config: MagicMock,
):
    get_session_config.return_value = None
    _get_config_path.return_value = Path("/path/that/does/not/exist")
    get_pass.return_value = "grtu..."

    with mock.patch.dict("os.environ", {}, clear=True):
        configure_session(api_key="prompt", cache="yes")
    get_pass.assert_called_once()
    write_config.assert_called_once()
    assert write_config.call_args[0][0].api_key == "grtu..."

    write_config.reset_mock()

    configure_session(api_key="grtu...")
    write_config.assert_not_called()


@patch("gretel_client.config._get_config_path")
def test_clear_gretel_config(_get_config_path: MagicMock):
    _get_config_path.return_value.exists.return_value = False
    with mock.patch.dict("os.environ", {}, clear=True):
        configure_session(clear=True)
    config_path = _get_config_path.return_value
    config_path.unlink.assert_called_once()
    config_path.parent.rmdir.assert_called_once()


@patch("urllib3.PoolManager")
@patch.dict(os.environ, {"GRETEL_API_KEY": "grtutest"})
def test_defaults_to_certifi_certs(pool_manager: MagicMock):
    config = ClientConfig.from_env()
    client = config.get_api(ProjectsApi)

    _, kwargs = pool_manager.call_args
    assert kwargs.get("ca_certs") == certifi.where()


@patch("urllib3.PoolManager")
@patch.dict(os.environ, {"GRETEL_API_KEY": "grtutest"})
def test_override_certs_via_environment_variables(pool_manager: MagicMock):
    with patch.dict(
        os.environ,
        {"SSL_CERT_FILE": "/ssl/cert/file"},
    ):
        config = ClientConfig.from_env()
        client = config.get_api(ProjectsApi)

        _, kwargs = pool_manager.call_args
        assert kwargs.get("ca_certs") == "/ssl/cert/file"

    with patch.dict(
        os.environ,
        {"REQUESTS_CA_BUNDLE": "/requests/ca/bundle"},
    ):
        config = ClientConfig.from_env()
        client = config.get_api(ProjectsApi)

        _, kwargs = pool_manager.call_args
        assert kwargs.get("ca_certs") == "/requests/ca/bundle"
