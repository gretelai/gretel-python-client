import pytest

from click.testing import CliRunner

from gretel_client_v2._cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_cli(runner):
    call = runner.invoke(cli, ["--help"])
    assert call.exit_code == 0
    assert call.output.startswith("Usage")
