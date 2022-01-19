from typing import Callable
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from gretel_client.cli.cli import cli
from gretel_client.config import get_session_config
from gretel_client.docker import AuthStrategy, DataVolumeDef


@patch("gretel_client.cli.connectors.build_container")
@patch("gretel_client.cli.common.get_project")
def test_does_construct_connector_container(
    get_project: MagicMock,
    build_container: MagicMock,
    get_fixture: Callable,
):
    run = CliRunner().invoke(
        cli,
        [
            "connectors",
            "start",
            "--project",
            "1234",
            "--model",
            "123",
            "--config",
            get_fixture("connectors/s3.yaml"),
        ],
    )

    sess = get_session_config()

    build_container.assert_called_once_with(
        image="gretelai/connector:dev",
        auth_strategy=AuthStrategy.AUTH_AND_RESOLVE,
        params={
            "--project": "1234",
            "--model": "123",
            "--config": "/etc/gretel//connector.yaml",
        },
        volumes=[
            DataVolumeDef(
                target_dir="/etc/gretel/",
                host_files=[(get_fixture("connectors/s3.yaml"), "connector.yaml")],
            )
        ],
        env={"GRETEL_API_KEY": sess.api_key, "GRETEL_ENDPOINT": sess.endpoint},
    )

    build_container.return_value.start.assert_called_once()

    assert run.exit_code == 0
