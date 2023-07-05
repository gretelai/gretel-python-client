import json
import os
import tempfile

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
    GRETEL_RUNNER_MODE,
    GretelClientConfigurationError,
    PreviewFeatures,
    RunnerMode,
    write_config,
)
from gretel_client.rest.api.projects_api import ProjectsApi


def test_custom_artifact_endpoint_requires_hybrid_runner():
    with pytest.raises(GretelClientConfigurationError):
        ClientConfig(artifact_endpoint="s3://my-bucket")


def test_hybrid_runner_cannot_run_without_custom_artifact_endpoint():
    with pytest.raises(GretelClientConfigurationError):
        ClientConfig(default_runner="hybrid")


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
    assert config.default_runner == RunnerMode.CLOUD
    assert _load_config(config_path)


def test_env_runner_mode(monkeypatch):
    monkeypatch.setenv(GRETEL_RUNNER_MODE, "local")
    config = ClientConfig()
    assert config.default_runner == RunnerMode.LOCAL


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
    configure_session(ClientConfig.from_env())

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


@patch("gretel_client.config._get_config_path")
def test_configure_hybrid_session(_get_config_path, dev_ep):
    _get_config_path.return_value = Path("/path/that/does/not/exist")
    with patch.dict(os.environ, {}, clear=True):
        configure_session(
            api_key="grtu...",
            endpoint=dev_ep,
            default_runner="hybrid",
            artifact_endpoint="s3://my-bucket",
        )
    config = get_session_config()

    assert config.api_key == "grtu..."
    assert config.endpoint == dev_ep
    assert config.default_runner == RunnerMode.HYBRID
    assert config.artifact_endpoint == "s3://my-bucket"


@patch("gretel_client.config._get_config_path")
@pytest.mark.parametrize("cache", ["yes", "no"])
@pytest.mark.parametrize("api_key", ["prompt", "grtu...NEW", None])
def test_configure_session_cached_values_plus_overrides(
    _get_config_path, api_key, cache, dev_ep
):

    cached_config = {
        "api_key": "grtu...CACHED",
        "default_runner": "hybrid",
        "artifact_endpoint": "s3://bucket-abc",
        "endpoint": dev_ep,
    }
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        with open(tmp.name, "w") as f:
            json.dump(cached_config, f)

        _get_config_path.return_value = Path(tmp.name)

        configure_session(
            artifact_endpoint="s3://bucket-xyz",
            api_key=api_key,
            cache=cache,
        )
        config = get_session_config()

        cached_config_after = json.load(tmp)

        tmp.close()
        os.unlink(tmp.name)

    if api_key == "prompt" or api_key is None:
        # We pick up the cached api key
        expected_api_key = "grtu...CACHED"
    elif api_key == "grtu...NEW":
        # We set the explicit api key
        expected_api_key = "grtu...NEW"

    # Override values are set on session
    assert config.artifact_endpoint == "s3://bucket-xyz"
    assert config.api_key == expected_api_key
    # Keys not specified in configure_session inherit values from cached file
    assert config.default_runner == "hybrid"
    assert config.endpoint == dev_ep

    # Cached file is not mutated, even when cache="yes" (which only writes to disk if a file is not already present)
    assert cached_config_after == cached_config


@patch("gretel_client.config._get_config_path")
def test_configure_session_cached_values_plus_invalid_overrides(
    _get_config_path, dev_ep
):
    cached_config = {
        "api_key": "GRTU...",
        "default_runner": "cloud",
        "artifact_endpoint": "cloud",
        "endpoint": dev_ep,
    }
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        with open(tmp.name, "w") as f:
            json.dump(cached_config, f)

        _get_config_path.return_value = Path(tmp.name)

        with pytest.raises(GretelClientConfigurationError):
            configure_session(
                api_key="grtu...",
                artifact_endpoint="s3://bucket-xyz",
            )

        tmp.close()
        os.unlink(tmp.name)


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
