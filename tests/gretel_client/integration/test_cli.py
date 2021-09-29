import json
import os
import uuid

from contextlib import contextmanager
from multiprocessing import Process, Queue
from pathlib import Path
from signal import SIGINT
from threading import Timer
from typing import Callable
from unittest.mock import MagicMock, patch

import pytest

from click.testing import CliRunner

from gretel_client.cli.cli import cli
from gretel_client.config import (
    _load_config,
    ClientConfig,
    configure_session,
    DEFAULT_GRETEL_ENDPOINT,
    DEFAULT_RUNNER,
    GRETEL_API_KEY,
)
from gretel_client.projects.jobs import Status
from gretel_client.projects.models import Model
from gretel_client.projects.projects import get_project, Project
from gretel_client.projects.records import RecordHandler


@pytest.fixture
def runner() -> CliRunner:
    """Returns a CliRunner that can be used to invoke the CLI from
    unit tests.
    """
    return CliRunner(mix_stderr=False)


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


@patch("gretel_client.cli.cli.write_config")
def test_cli_does_configure(write_config: MagicMock, runner: CliRunner):
    cmd = runner.invoke(cli, ["configure"], input="\n\ngrtu...\n\n")
    assert not cmd.exception
    write_config.assert_called_once_with(
        ClientConfig(
            endpoint="https://api-dev.gretel.cloud",
            api_key="grtu...",
            default_project_name=None,
            default_runner=DEFAULT_RUNNER.value,
        )
    )


@patch("gretel_client.cli.cli.write_config")
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


@patch("gretel_client.cli.cli.write_config")
def test_cli_does_pass_configure_with_bad_project(
    write_config: MagicMock, runner: CliRunner
):
    with clear_session_config():
        cmd = runner.invoke(
            cli,
            ["configure"],
            input=f"{DEFAULT_GRETEL_ENDPOINT}\n\n{os.getenv(GRETEL_API_KEY)}\nbad-project-key\n",
            catch_exceptions=True,
        )
        assert cmd.exit_code == 0


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
    model = next(project.search_models(factory=dict))
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


def test_model_crud_from_cli_local_inputs(
    runner: CliRunner, project: Project, get_fixture: Callable, tmpdir: Path
):
    # this test looks similar to test_model_crud_from_cli but will instead
    # test a training run using local inputs and outputs.

    # 1. create a new model and run it locally.
    cmd = runner.invoke(
        cli,
        [
            "models",
            "create",
            "--config",
            str(get_fixture("transforms_config_local_datasource.yml")),
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
    model = next(project.search_models(factory=dict))
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
    assert "Usage:" in cmd.stderr and "--output is not set" in cmd.stderr

    # check that --runner=local and --output params are ok
    cmd = runner.invoke(cli, base_cmd + ["--output", "tmp"])
    assert cmd.exit_code == 0

    # check that --wait cant be passed with an output dir
    cmd = runner.invoke(cli, base_cmd + ["--output", "tmp", "--wait", "10"])
    assert cmd.exit_code == 2 and "--wait is >= 0" in cmd.stderr


def test_manual_model_params(
    runner: CliRunner, project: Project, get_fixture: Callable
):
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
        "Usage:" in cmd.stderr
        and "--runner manual cannot be used together with" in cmd.stderr
        and "--in-data" in cmd.stderr
    )

    # check that --runner=local and --output params are ok
    cmd = runner.invoke(cli, base_cmd + ["--output", "tmp"])
    assert (
        "Usage:" in cmd.stderr
        and "--runner manual cannot be used together with" in cmd.stderr
        and "--output" in cmd.stderr
    )
    assert cmd.exit_code == 2

    # check that --wait cant be passed with an output dir
    cmd = runner.invoke(cli, base_cmd + ["--upload-model"])
    assert (
        "Usage:" in cmd.stderr
        and "--runner manual cannot be used together with" in cmd.stderr
        and "--upload-model" in cmd.stderr
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
    assert "gretel_" in cmd.stderr  # checks that a gretel key is returneds
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


def test_records_transform(
    runner: CliRunner, get_fixture: Callable, tmpdir: Path, trained_xf_model: Model
):
    cmd = runner.invoke(
        cli,
        [  # type:ignore
            "records",
            "transform",
            "--model-id",
            trained_xf_model.model_id,
            "--project",
            trained_xf_model.project.project_id,
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


def test_records_classify(
    runner: CliRunner,
    get_fixture: Callable,
    tmpdir: Path,
    trained_classify_model: Model,
):
    cmd = runner.invoke(
        cli,
        [  # type:ignore
            "records",
            "classify",
            "--project",
            trained_classify_model.project.project_id,
            "--model-id",
            trained_classify_model.model_id,
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


def test_invalid_project_create(runner: CliRunner):
    project_name = f"{uuid.uuid4().hex[:5]}_not_dns_compliant"
    cmd = runner.invoke(cli, ["--debug", "projects", "create", "--name", project_name])
    assert cmd.exit_code == 1


def test_can_create_project(runner: CliRunner, request):
    project_name = f"{uuid.uuid4().hex[:5]}"
    request.addfinalizer(lambda: get_project(name=project_name).delete())
    cmd = runner.invoke(
        cli,
        [
            "--debug",
            "projects",
            "create",
            "--name",
            project_name,
            "--display-name",
            "test-project",
            "--desc",
            "test description",
        ],
    )
    assert cmd.exit_code == 0


def test_create_records_from_model_obj(
    runner: CliRunner, project: Project, get_fixture: Callable, tmpdir: Path
):
    cmd = runner.invoke(
        cli,
        [
            "models",
            "create",
            "--config",
            str(get_fixture("classify_config.yml")),
            "--output",
            str(tmpdir),
            "--project",
            project.project_id,
            "--runner",
            "local",
            "--in-data",
            str(get_fixture("account-balances.csv")),
        ],
    )
    print(cmd.output)
    assert cmd.exit_code == 0
    assert (tmpdir / "model.tar.gz").exists()
    model_obj = Path(tmpdir / "model_obj.json")
    model_obj.write_text(cmd.stdout)
    cmd = runner.invoke(
        cli,
        [
            "records",
            "classify",
            "--output",
            str(tmpdir),
            "--model-id",
            str(model_obj),
            "--model-path",
            str(tmpdir / "model.tar.gz"),
        ],
    )

    assert cmd.exit_code == 0


def test_does_not_download_cloud_model_data(
    runner: CliRunner, get_fixture: Callable, tmpdir: Path, trained_synth_model: Model
):
    cmd = runner.invoke(
        cli,
        [
            "models",
            "get",
            "--model-id",
            trained_synth_model.model_id,
            "--project",
            trained_synth_model.project.project_id,
            "--output",
            str(tmpdir / "downloaded"),
        ],
    )
    assert cmd.exit_code == 0
    assert not (tmpdir / "downloaded" / "model.tar.gz").exists()


def test_manual_record_handler_cleanup(
    runner: CliRunner, get_fixture: Callable, tmpdir: Path, trained_synth_model: Model
):
    """
    Setup of this test is following:
    - run a background process that will run a command and which will receive a SIGINT signal after 10 secs
    - SIGINT should interrupt the run and cancel the job
    - because we use manual runner, the worker_key should be outputted
    """

    def _generate_with_manual_runner(queue: Queue):
        # kill the process after 10 seconds
        Timer(10, lambda: os.kill(os.getpid(), SIGINT)).start()
        cmd = runner.invoke(
            cli,
            [  # type:ignore
                "records",
                "generate",
                "--project",
                trained_synth_model.project.project_id,
                "--model-id",
                trained_synth_model.model_id,
                "--runner",
                "manual",
            ],
        )
        queue.put({"out": cmd.stdout, "err": cmd.stderr})

    process_queue = Queue()
    process = Process(target=_generate_with_manual_runner, args=(process_queue,))
    process.start()
    process.join()

    out = process_queue.get()

    command_out = json.loads(out["out"])
    assert "record_handler" in command_out

    record_handler_id = command_out["record_handler"].get("uid")
    assert record_handler_id is not None

    # it's manual mode, so worker_key should be outputted
    assert "worker_key" in command_out

    assert "Got interrupt signal." in out["err"]
    assert "Attempting graceful shutdown" in out["err"]

    record_handler = RecordHandler(trained_synth_model, record_id=record_handler_id)
    assert record_handler.status == Status.CANCELLED
