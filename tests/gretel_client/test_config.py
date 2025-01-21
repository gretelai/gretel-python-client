import json
import os
import tempfile

from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch
from urllib.parse import unquote_plus

import certifi
import pytest

from urllib3 import HTTPResponse
from urllib3.exceptions import MaxRetryError

from gretel_client.config import (
    _get_client_version,
    _load_config,
    add_session_context,
    CLIENT_METRICS_HEADER_KEY,
    ClientConfig,
    configure_session,
    get_session_config,
    GRETEL_PREVIEW_FEATURES,
    GRETEL_RUNNER_MODE,
    GretelApiRetry,
    GretelClientConfigurationError,
    PreviewFeatures,
    RunnerMode,
    write_config,
)
from gretel_client.rest.api.projects_api import ProjectsApi
from gretel_client.rest.exceptions import ForbiddenException
from gretel_client.rest_v1.models.list_serverless_tenants_response import (
    ListServerlessTenantsResponse,
)


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
        configure_session(config, validate=False)
        assert get_session_config() == config
    finally:
        configure_session(_load_config(), validate=False)


@patch.dict(os.environ, {"GRETEL_API_KEY": "grtutest"})
def test_can_get_api_bindings():
    configure_session(ClientConfig.from_env(), validate=False)

    client = get_session_config()
    assert isinstance(client.get_api(ProjectsApi), ProjectsApi)


@patch.dict(
    os.environ, {"GRETEL_API_KEY": "grtutest", "http_proxy": "http://localhost:8080"}
)
def test_proxy_set_http():
    configure_session(ClientConfig.from_env(), validate=False)

    config = get_session_config()
    client = config.get_api(ProjectsApi)
    assert client.api_client.configuration.proxy == "http://localhost:8080"


@patch.dict(
    os.environ, {"GRETEL_API_KEY": "grtutest", "https_proxy": "https://localhost:8080"}
)
def test_proxy_set_https():
    configure_session(ClientConfig.from_env(), validate=False)

    config = get_session_config()
    client = config.get_api(ProjectsApi)
    assert client.api_client.configuration.proxy == "https://localhost:8080"


@patch.dict(
    os.environ,
    {
        "GRETEL_API_KEY": "grtutest",
        "all_proxy": "http://localhost:9999",
        "https_proxy": "https://localhost:8080",
    },
)
def test_proxy_set_all_proxy():
    configure_session(ClientConfig.from_env(), validate=False)

    config = get_session_config()
    client = config.get_api(ProjectsApi)
    assert client.api_client.configuration.proxy == "http://localhost:9999"


def test_configure_preview_features():
    with patch.dict(
        os.environ,
        {GRETEL_PREVIEW_FEATURES: PreviewFeatures.ENABLED.value},
    ):
        configure_session(
            _load_config(),
            validate=False,
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
        configure_session(api_key="prompt", cache="yes", validate=False)
    get_pass.assert_called_once()
    write_config.assert_called_once()
    assert write_config.call_args[0][0].api_key == "grtu..."

    write_config.reset_mock()

    configure_session(api_key="grtu...", validate=False)
    write_config.assert_not_called()


@patch("gretel_client.config._get_config_path")
def test_clear_gretel_config(_get_config_path: MagicMock):
    _get_config_path.return_value.exists.return_value = False
    with mock.patch.dict("os.environ", {}, clear=True):
        configure_session(clear=True, validate=False)
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
            validate=False,
        )
    config = get_session_config()

    assert config.api_key == "grtu..."
    assert config.endpoint == dev_ep
    assert config.default_runner == RunnerMode.HYBRID
    assert config.artifact_endpoint == "s3://my-bucket"


@patch("gretel_client.config._get_config_path")
def test_custom_artifact_endpoint_strips_trailing_slash(_get_config_path, dev_ep):
    _get_config_path.return_value = Path("/path/that/does/not/exist")
    with patch.dict(os.environ, {}, clear=True):
        configure_session(
            api_key="grtu...",
            endpoint=dev_ep,
            default_runner="hybrid",
            artifact_endpoint="s3://my-bucket/",
            validate=False,
        )
    config = get_session_config()

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
            validate=False,
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
                validate=False,
            )

        tmp.close()
        os.unlink(tmp.name)


def test_metrics_headers(dev_ep):
    def _get_headers(api):
        return api.api_client.default_headers

    def get_metrics_header(session):
        api = session.get_api(ProjectsApi)
        return unquote_plus(_get_headers(api)[CLIENT_METRICS_HEADER_KEY])

    configure_session(api_key="grtu...", validate=False)

    common = f"python_sdk_version={_get_client_version()}"

    # Always include the sdk version
    assert get_metrics_header(get_session_config()) == common

    # Tagged sessions append extra metadata
    tagged = add_session_context(session=None, client_metrics={"hello": "world"})
    assert get_metrics_header(tagged) == f"{common};hello=world"

    # Tagged sessions can be composed
    multi_tagged = add_session_context(session=tagged, client_metrics={"foo": "bar"})
    assert get_metrics_header(multi_tagged) == f"{common};hello=world;foo=bar"

    # add_session_context and resultant Tagged sessions are None-safe
    no_extra_metrics = add_session_context()
    assert get_metrics_header(no_extra_metrics) == common


@patch("urllib3.PoolManager")
@patch.dict(os.environ, {"GRETEL_API_KEY": "grtutest"})
def test_defaults_to_certifi_certs(pool_manager: MagicMock):
    config = ClientConfig.from_env()
    config.get_api(ProjectsApi)

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


def test_dont_retry_non_throttling_403s():
    retry = GretelApiRetry.create_default(max_retry_attempts=3, backoff_factor=0.1)

    result = retry.increment(
        "GET",
        "/url",
        HTTPResponse(
            body=b'{"message":"User is not authorized to access this resource"}',
            status=403,
        ),
        error=ForbiddenException(),
    )

    # that call should use up one retry
    assert result.total == 2


def test_dont_retry_no_response():
    retry = GretelApiRetry.create_default(max_retry_attempts=3, backoff_factor=0.1)

    result = retry.increment(
        "GET",
        "/url",
        response=None,
        error=ForbiddenException(),
    )

    # that call should use up one retry and not fail
    assert result.total == 2


def test_retry_throttling_403s():
    retry = GretelApiRetry.create_default(max_retry_attempts=3, backoff_factor=0.1)

    error = ForbiddenException()
    with pytest.raises(MaxRetryError) as e:
        retry.increment(
            "GET",
            "/url",
            HTTPResponse(body=b'{"message":"Access denied"}', status=403),
            error=error,
        )

    assert e.value.reason == error


# Test when a user configures an enterprise tenant that it should call set_serverless_api
def test_configure_enterprise():
    cfg = ClientConfig(
        endpoint="localhost:8080",
        api_key="grtu...",
        tenant_name="enterprise",
    )
    with patch.object(ClientConfig, "set_serverless_api") as set_serverless_api:
        with patch.object(ClientConfig, "email"):
            set_serverless_api.return_value = True
            configure_session(cfg)
            set_serverless_api.assert_called_once()


# Test when tenant name is provided and validate is false, should throw a client config error
def test_configure_enterprise_validate_false():
    cfg = ClientConfig(
        endpoint="localhost:8080",
        api_key="grtu...",
        tenant_name="enterprise",
    )
    with patch.object(ClientConfig, "set_serverless_api") as set_serverless_api:
        with patch.object(ClientConfig, "email"):
            set_serverless_api.return_value = True
            with pytest.raises(GretelClientConfigurationError):
                configure_session(cfg, validate=False)
            set_serverless_api.assert_not_called


# Test when a list is returned and a matching tenant name is found
def test_set_serverless_api_tenant_found_match():
    cfg = ClientConfig(
        endpoint="localhost:8080",
        api_key="grtu...",
        tenant_name="enterprise",
    )
    cfg.get_v1_api = MagicMock()
    cfg.get_v1_api.return_value = MagicMock()
    data = '{"tenants":[{"guid":"1","domain_guid":"fake","created_at":"2024-11-18T17:45:27.189+00:00","cloud_provider":{"provider":"AWS","region":"us-west-2"},"config":{"cell_id":"1","api_endpoint":"gretelhost:8080"},"name":"enterprise"}]}'
    cfg.get_v1_api.return_value.list_serverless_tenants.return_value = (
        ListServerlessTenantsResponse.from_json(data)
    )
    assert cfg.set_serverless_api()
    assert cfg.endpoint == "https://gretelhost:8080"


# Test when no tenants are returned
def test_set_serverless_api_tenant_no_tenants():
    cfg = ClientConfig(
        endpoint="localhost:8080",
        api_key="grtu...",
        tenant_name="enterprise",
    )
    cfg.get_v1_api = MagicMock()
    cfg.get_v1_api.return_value = MagicMock()
    data = '{"tenants":[]}'
    cfg.get_v1_api.return_value.list_serverless_tenants.return_value = (
        ListServerlessTenantsResponse.from_json(data)
    )
    assert not cfg.set_serverless_api()
    assert cfg.endpoint == "localhost:8080"


# Test when a list is returned but no matching tenant name
def test_set_serverless_api_tenant_no_match():
    cfg = ClientConfig(
        endpoint="localhost:8080",
        api_key="grtu...",
        tenant_name="enterprise",
    )
    cfg.get_v1_api = MagicMock()
    cfg.get_v1_api.return_value = MagicMock()
    data = '{"tenants":[{"guid":"1","domain_guid":"fake","created_at":"2024-11-18T17:45:27.189+00:00","cloud_provider":{"provider":"AWS","region":"us-west-2"},"config":{"cell_id":"1","api_endpoint":"gretelhost:8080"},"name":"other"}]}'
    cfg.get_v1_api.return_value.list_serverless_tenants.return_value = (
        ListServerlessTenantsResponse.from_json(data)
    )
    assert not cfg.set_serverless_api()
    assert cfg.endpoint == "localhost:8080"
