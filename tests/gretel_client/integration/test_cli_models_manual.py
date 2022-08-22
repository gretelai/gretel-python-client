import json
import multiprocessing as mp
import os

from queue import Queue
from signal import SIGINT
from threading import Timer
from typing import Callable

from click.testing import CliRunner

from gretel_client.cli.cli import cli
from gretel_client.projects.jobs import Status
from gretel_client.projects.models import Model
from gretel_client.projects.projects import Project
from gretel_client.projects.records import RecordHandler

from .conftest import print_cmd_output, pytest_skip_on_windows


def test_model_crud_manual_mode(project: Project, get_fixture: Callable):
    # This test is using it's own runner and uses separate capture for STDOUT and STDERR.
    # This is to verify that only JSON object is outputted on STDOUT, so the CLI can be
    # piped with other commands.
    runner = CliRunner(mix_stderr=False)

    # 1. create a new model with manual runner_mode and --wait=0
    cmd = runner.invoke(
        cli,
        [
            "--debug",
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
    print_cmd_output(cmd)
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
    print_cmd_output(cmd)
    assert cmd.exit_code == 0


def test_manual_model_params(runner: CliRunner, project: Project):
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


@pytest_skip_on_windows
def test_manual_record_handler_cleanup(runner: CliRunner, trained_synth_model: Model):
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

    ctx = mp.get_context("fork")
    process_queue = ctx.Queue()
    process = ctx.Process(target=_generate_with_manual_runner, args=(process_queue,))
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
