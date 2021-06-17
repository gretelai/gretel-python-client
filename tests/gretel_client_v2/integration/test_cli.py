import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from gretel_client_v2.cli.cli import cli
from gretel_client_v2.config import (
    DEFAULT_GRETEL_ENDPOINT,
    DEFAULT_RUNNER,
    GRETEL_API_KEY,
    ClientConfig,
    _load_config,
    configure_session,
)
from gretel_client_v2.projects.docker import _is_inside_container
from gretel_client_v2.projects.models import Model
from gretel_client_v2.projects.projects import Project, get_project


@pytest.fixture
def runner() -> CliRunner:
    """Returns a CliRunner that can be used to invoke the CLI from
    unit tests.
    """
    return CliRunner()


@contextmanager
def clear_session_config():
    """Clears any session config so we can simulate a host without
    an existing Gretel config.
    """
    with patch.dict(os.environ, {}, clear=True):
        configure_session(ClientConfig())
    yield
    configure_session(_load_config())


def test_cli(runner):
    cmd = runner.invoke(cli, ["--help"])
    assert cmd.exit_code == 0
    assert cmd.output.startswith("Usage")


@patch("gretel_client_v2.cli.cli.write_config")
def test_cli_does_configure(write_config: MagicMock, runner: CliRunner):
    cmd = runner.invoke(cli, ["configure"], input="\n\ngrtu...\n\n")
    assert not cmd.exception
    write_config.assert_called_once_with(
        ClientConfig(
            endpoint="https://api.gretel.cloud",
            api_key="grtu...",
            default_project_name=None,
            default_runner=DEFAULT_RUNNER.value,
        )
    )


@patch("gretel_client_v2.cli.cli.write_config")
def test_cli_does_configure_with_project(
    write_config: MagicMock, runner: CliRunner, project: Project
):
    with clear_session_config():
        cmd = runner.invoke(
            cli,
            ["configure"],
            input=f"https://api-dev.gretel.cloud\n\n{os.getenv(GRETEL_API_KEY)}\n{project.name}\n",
            catch_exceptions=True,
        )
    assert not cmd.exception
    assert cmd.exit_code == 0
    write_config.assert_called_once_with(
        ClientConfig(
            endpoint="https://api-dev.gretel.cloud",
            api_key=os.getenv(GRETEL_API_KEY),
            default_project_name=project.name,
        )
    )


@patch("gretel_client_v2.cli.cli.write_config")
def test_cli_does_fail_configure_with_bad_project(
    write_config: MagicMock, runner: CliRunner
):
    with clear_session_config():
        cmd = runner.invoke(
            cli,
            ["configure"],
            input=f"{DEFAULT_GRETEL_ENDPOINT}\n\n{os.getenv(GRETEL_API_KEY)}\nbad-project-key\n",
            catch_exceptions=True,
        )
        assert cmd.exit_code == 1


def test_model_crud_from_cli(
    runner: CliRunner, project: Project, get_fixture: Callable, tmpdir: Path
):
    # 1. create a new model and run it locally.
    cmd = runner.invoke(
        cli,
        [
            "models",
            "create",
            "--config",
            str(get_fixture("transforms_config.yml")),
            "--project",
            project.project_id,
            "--runner",
            "local",
            "--output",
            str(tmpdir),
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0
    # 2. check that the model can be found via a search
    model = project.search_models()[0]
    model_id = model["uid"]
    assert model["status"] == "completed"
    assert not model["error_msg"]
    cmd = runner.invoke(
        cli,
        [
            "models",
            "search",
            "--project",
            project.project_id,
        ],
    )
    print(cmd.output)
    assert model_id in cmd.output
    assert cmd.exit_code == 0
    # 3. check that an existing model can be downloaded back to disk
    cmd = runner.invoke(
        cli,
        [
            "models",
            "get",
            "--model-id",
            model_id,
            "--project",
            project.project_id,
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0
    # 4. check that the model can be deleted
    cmd = runner.invoke(
        cli,
        [
            "models",
            "delete",
            "--model-id",
            model_id,
            "--project",
            project.project_id,
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0


def test_model_crud_manual_mode(project: Project, get_fixture: Callable, tmpdir: Path):
    # This test is using it's own runner and uses separate capture for STDOUT and STDERR.
    # This is to verify that only JSON object is outputted on STDOUT, so the CLI can be
    # piped with other commands.
    runner = CliRunner(mix_stderr=False)

    # 1. create a new model with manual runner_mode and --wait=0
    cmd = runner.invoke(
        cli,
        [
            "models",
            "create",
            "--config",
            str(get_fixture("transforms_config.yml")),
            "--project",
            project.project_id,
            "--runner",
            "manual",
            "--wait",
            "0",
        ],
    )
    print(cmd.stdout)
    print(cmd.stderr)
    assert cmd.exit_code == 0
    assert "not waiting for the job completion" in cmd.stderr

    output_json = json.loads(cmd.stdout)
    model_id = output_json["model"]["uid"]
    assert output_json.get("worker_key") is not None

    # 2. check that the model can be found via a get
    model = project.get_model(model_id=model_id)
    assert model is not None
    assert model.status == "created"
    assert model.runner_mode == "manual"

    # 3. delete the model
    cmd = runner.invoke(
        cli,
        [
            "models",
            "delete",
            "--model-id",
            model_id,
            "--project",
            project.project_id,
        ],
    )
    print(cmd.stdout)
    print(cmd.stderr)
    assert cmd.exit_code == 0


@pytest.mark.skipif(_is_inside_container(), reason="running test from docker")
def test_model_crud_from_cli_local_inputs(
    runner: CliRunner, project: Project, get_fixture: Callable, tmpdir: Path
):
    # this test looks similar to test_model_crud_from_cli but will instead
    # test a training run using local inputs and outputs. currently, we don't
    # support mounting volumes when the cli is running inside a docker
    # container. the following check ensures that this test is only run
    # when the test harness is running on a non-docker host.

    # 1. create a new model and run it locally.
    cmd = runner.invoke(
        cli,
        [
            "models",
            "create",
            "--config",
            str(get_fixture("transforms_config.yml")),
            "--in-data",
            str(get_fixture("account-balances.csv")),
            "--output",
            str(tmpdir),
            "--project",
            project.project_id,
            "--runner",
            "local",
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0
    assert (tmpdir / "model.tar.gz").exists()
    assert (tmpdir / "model_logs.json.gz").exists()
    # 2. check that the model can be found via a search
    model = project.search_models()[0]
    model_id = model["uid"]

    assert model["status"] == "completed"
    assert not model["error_msg"]

    cmd = runner.invoke(
        cli,
        [
            "models",
            "search",
            "--project",
            project.project_id,
        ],
    )
    print(cmd.output)
    assert model_id in cmd.output
    assert cmd.exit_code == 0

    # 3. run model
    cmd = runner.invoke(
        cli,
        [
            "records",
            "transform",
            "--project",
            project.project_id,
            "--model-id",
            model_id,
            "--in-data",
            str(get_fixture("account-balances.csv")),
            "--output",
            str(tmpdir / "record_handler"),
            "--model-path",
            str(tmpdir / "model.tar.gz"),
            "--runner",
            "local",
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0
    assert (tmpdir / "record_handler/data.gz").exists()

    # 4. check that an existing model can be downloaded back to disk
    output_dir = tmpdir / "from_existing"
    cmd = runner.invoke(
        cli,
        [
            "models",
            "get",
            "--model-id",
            model_id,
            "--output",
            str(output_dir),
            "--project",
            project.project_id,
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0
    assert (output_dir / "logs.json.gz").exists()

    # 5. check that the model can be deleted
    cmd = runner.invoke(
        cli,
        [
            "models",
            "delete",
            "--model-id",
            model_id,
            "--project",
            project.project_id,
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0


def test_local_model_params(runner: CliRunner, project: Project, get_fixture: Callable):
    base_cmd = [
        "models",
        "create",
        "--config",
        "synthetics/default",
        "--in-data",
        str(get_fixture("account-balances.csv")),
        "--runner",
        "local",
        "--dry-run",
        "--project",
        project.project_id,
    ]

    # assert that --runner=local and no output param results in an error
    cmd = runner.invoke(cli, base_cmd)
    assert cmd.exit_code == 2
    assert "Usage:" in cmd.output and "--output is not set" in cmd.output

    # check that --runner=local and --output params are ok
    cmd = runner.invoke(cli, base_cmd + ["--output", "tmp"])
    assert cmd.exit_code == 0

    # check that --wait cant be passed with an output dir
    cmd = runner.invoke(cli, base_cmd + ["--output", "tmp", "--wait", "10"])
    assert cmd.exit_code == 2 and "--wait is >= 0" in cmd.output


def test_manual_model_params(runner: CliRunner, project: Project, get_fixture: Callable):
    base_cmd = [
        "models",
        "create",
        "--config",
        "synthetics/default",
        "--runner",
        "manual",
        "--dry-run",
        "--project",
        project.project_id,
    ]

    # assert that --runner=manual and in-data param results in an error
    cmd = runner.invoke(
        cli, base_cmd + ["--in-data", str(get_fixture("account-balances.csv"))]
    )
    assert cmd.exit_code == 2
    assert (
        "Usage:" in cmd.output
        and "--runner manual cannot be used together with" in cmd.output
        and "--in-data" in cmd.output
    )

    # check that --runner=local and --output params are ok
    cmd = runner.invoke(cli, base_cmd + ["--output", "tmp"])
    assert (
        "Usage:" in cmd.output
        and "--runner manual cannot be used together with" in cmd.output
        and "--output" in cmd.output
    )
    assert cmd.exit_code == 2

    # check that --wait cant be passed with an output dir
    cmd = runner.invoke(cli, base_cmd + ["--upload-model"])
    assert (
        "Usage:" in cmd.output
        and "--runner manual cannot be used together with" in cmd.output
        and "--upload-model" in cmd.output
    )
    assert cmd.exit_code == 2


def test_artifacts_crud(runner: CliRunner, project: Project, get_fixture: Callable):
    # upload an artifact
    cmd = runner.invoke(
        cli,
        [
            "artifacts",
            "upload",
            "--project",
            project.name,
            "--in-data",
            get_fixture("account-balances.csv"),
        ],
    )
    assert "gretel_" in cmd.output  # checks that a gretel key is returneds
    assert cmd.exit_code == 0
    assert len(project.artifacts) == 1
    # check that we can list the artifact
    cmd = runner.invoke(cli, ["artifacts", "list", "--project", project.name])
    assert "account-balances" in cmd.output
    assert cmd.exit_code == 0
    assert len(project.artifacts) == 1
    # check that we can delete the artifact
    cmd = runner.invoke(
        cli,
        [
            "artifacts",
            "delete",
            "--project",
            project.name,
            "--artifact-key",
            project.artifacts[0]["key"],
        ],
    )
    assert cmd.exit_code == 0
    assert len(project.artifacts) == 0


def test_artifact_invalid_data(
    runner: CliRunner, project: Project, get_fixture: Callable
):
    cmd = runner.invoke(
        cli,
        [
            "artifacts",
            "upload",
            "--project",
            project.name,
            "--in-data",
            get_fixture("invalid_data.json"),
        ],
    )
    assert (
        cmd.exit_code == 0
    )  # todo(dn): this should fail when we get better data validation checks


@pytest.mark.skipif(_is_inside_container(), reason="running test from docker")
def test_records_generate(
    runner: CliRunner, get_fixture: Callable, tmpdir: Path, trained_synth_model: Model
):
    cmd = runner.invoke(
        cli,
        [  # type:ignore
            "records",
            "generate",
            "--project",
            trained_synth_model.project.project_id,
            "--model-id",
            trained_synth_model.model_id,
            "--output",
            str(tmpdir),
            "--runner",
            "local",
        ],
    )
    assert cmd.exit_code == 0
    assert (tmpdir / "data.gz").exists()


@pytest.mark.skipif(_is_inside_container(), reason="running test from docker")
def test_records_transform(
    runner: CliRunner, get_fixture: Callable, tmpdir: Path, trained_xf_model: Model
):
    cmd = runner.invoke(
        cli,
        [  # type:ignore
            "records",
            "transform",
            "--project",
            trained_xf_model.project.project_id,
            "--model-id",
            trained_xf_model.model_id,
            "--in-data",
            str(get_fixture("account-balances.csv")),
            "--output",
            str(tmpdir),
            "--runner",
            "local",
        ],
    )
    assert cmd.exit_code == 0
    assert (tmpdir / "data.gz").exists()
