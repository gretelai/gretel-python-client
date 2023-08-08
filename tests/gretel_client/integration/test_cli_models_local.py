import re

from pathlib import Path
from typing import Callable

from click.testing import CliRunner

from gretel_client.cli.cli import cli
from gretel_client.projects.models import Model
from gretel_client.projects.projects import Project

from .conftest import print_cmd_output, pytest_skip_on_windows


@pytest_skip_on_windows
def test_model_crud_from_cli_local_inputs(
    runner: CliRunner, get_fixture: Callable, project: Project, tmpdir: Path
):
    # this test looks similar to test_model_crud_from_cli but will instead
    # test a training run using local inputs and outputs.

    # 1. create a new model and run it locally.
    cmd = runner.invoke(
        cli,
        [
            "--debug",
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
            "--ref-data",
            str(get_fixture("account-balances.csv")),
        ],
    )
    print_cmd_output(cmd)
    assert cmd.exit_code == 0
    assert (tmpdir / "model.tar.gz").exists()
    assert (tmpdir / "model_logs.json.gz").exists()

    # 2. check that the model can be found via a search
    model = next(project.search_models(factory=dict))
    model_id = model["uid"]
    config = list(model["config"]["models"][0].values())[0]
    assert config["ref_data"]["0"].endswith("account-balances.csv")

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
    print_cmd_output(cmd)
    assert model_id in cmd.output
    assert cmd.exit_code == 0

    # 3. run model
    cmd = runner.invoke(
        cli,
        [
            "--debug",
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
    print_cmd_output(cmd)
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


@pytest_skip_on_windows
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


@pytest_skip_on_windows
def test_records_generate_and_get_record_handler(
    runner: CliRunner, get_fixture: Callable, tmpdir: Path, trained_synth_model: Model
):
    # Records generation
    cmd = runner.invoke(
        cli,
        [  # type:ignore
            "--debug",
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
    print_cmd_output(cmd)
    assert cmd.exit_code == 0
    assert (tmpdir / "data.gz").exists()

    # Get record artifacts
    record_id = re.findall(r'"uid": "[a-z,0-9]+', cmd.output)[0].split('"')[-1]
    cmd = runner.invoke(
        cli,
        [  # type:ignore
            "--debug",
            "records",
            "get",
            "--project",
            trained_synth_model.project.project_id,
            "--model-id",
            trained_synth_model.model_id,
            "--record-handler-id",
            record_id,
            "--output",
            str(tmpdir),
        ],
    )
    print_cmd_output(cmd)
    assert cmd.exit_code == 0
    assert (tmpdir / "data.gz").exists()
    assert (tmpdir / "run_logs.json.gz").exists()


@pytest_skip_on_windows
def test_records_generate_with_model_run(
    runner: CliRunner, get_fixture: Callable, tmpdir: Path, trained_synth_model: Model
):
    cmd = runner.invoke(
        cli,
        [  # type:ignore
            "--debug",
            "models",
            "run",
            "--project",
            trained_synth_model.project.project_id,
            "--model-id",
            trained_synth_model.model_id,
            "--output",
            str(tmpdir),
            "--runner",
            "local",
            "--param",
            "num_records",
            100,
            "--param",
            "max_invalid",
            100,
        ],
    )
    print_cmd_output(cmd)
    assert cmd.exit_code == 0
    assert (tmpdir / "data.gz").exists()


@pytest_skip_on_windows
def test_records_transform(
    runner: CliRunner, get_fixture: Callable, tmpdir: Path, trained_xf_model: Model
):
    cmd = runner.invoke(
        cli,
        [  # type:ignore
            "--debug",
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
    print_cmd_output(cmd)
    assert cmd.exit_code == 0
    assert (tmpdir / "data.gz").exists()


@pytest_skip_on_windows
def test_records_transform_with_model_run(
    runner: CliRunner, get_fixture: Callable, tmpdir: Path, trained_xf_model: Model
):
    cmd = runner.invoke(
        cli,
        [  # type:ignore
            "--debug",
            "models",
            "run",
            "--model-id",
            trained_xf_model.model_id,
            "--project",
            trained_xf_model.project.project_id,
            "--in-data",
            str(get_fixture("account-balances.csv")),
            "--ref-data",
            str(get_fixture("account-balances.csv")),
            "--output",
            str(tmpdir),
            "--runner",
            "local",
        ],
    )
    print_cmd_output(cmd)
    assert cmd.exit_code == 0
    assert (tmpdir / "data.gz").exists()


@pytest_skip_on_windows
def test_records_classify(
    runner: CliRunner,
    get_fixture: Callable,
    tmpdir: Path,
    trained_classify_model: Model,
):
    cmd = runner.invoke(
        cli,
        [  # type:ignore
            "--debug",
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
    print_cmd_output(cmd)
    assert cmd.exit_code == 0
    assert (tmpdir / "data.gz").exists()


@pytest_skip_on_windows
def test_create_records_from_model_obj(
    runner: CliRunner, project: Project, get_fixture: Callable, tmpdir: Path
):
    cmd = runner.invoke(
        cli,
        [
            "--debug",
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
    print_cmd_output(cmd)
    assert cmd.exit_code == 0
    assert (tmpdir / "model.tar.gz").exists()
    model_obj = Path(tmpdir / "model_obj.json")
    model_obj.write_text(cmd.stdout)
    cmd = runner.invoke(
        cli,
        [
            "--debug",
            "records",
            "classify",
            "--output",
            str(tmpdir),
            "--model-id",
            str(model_obj),
            "--project",
            project.project_id,
            "--model-path",
            str(tmpdir / "model.tar.gz"),
            "--in-data",
            str(get_fixture("account-balances.csv")),
        ],
    )
    print_cmd_output(cmd)
    assert cmd.exit_code == 0
