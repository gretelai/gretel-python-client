from pathlib import Path

from gretel_client_v2.config import (
    _ClientConfig,
    write_config,
    _load_config,
    get_session_config,
    configure_session,
)


def test_does_read_and_write_config(tmpdir):
    config = _ClientConfig(
        endpoint="api-dev.gretel.cloud",
        api_key="grtu...",
        default_project_name=None,
    )

    tmp_config_path = Path(tmpdir / "config.json")
    config_path = write_config(config, config_path=tmp_config_path)
    assert config_path == tmp_config_path
    assert _load_config(config_path)


def test_does_set_session_factory():
    config = _ClientConfig(
        endpoint="api-dev.gretel.cloud",
        api_key="grtu...",
        default_project_name=None,
    )
    assert get_session_config() != config
    configure_session(config)
    assert get_session_config() == config
