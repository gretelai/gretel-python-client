from pathlib import Path
from typing import Callable

from click.testing import CliRunner

from gretel_client.cli.cli import cli
from gretel_client.projects.models import Model
from gretel_client.projects.projects import Project

from .conftest import print_cmd_output, pytest_skip_on_windows


@pytest_skip_on_windows
def test_model_crud_from_cli(
    runner: CliRunner, project: Project, get_fixture: Callable, tmpdir: Path
):
    # 1. create a new model and run it locally.
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
            "local",
            "--output",
            str(tmpdir),
        ],
    )
    print_cmd_output(cmd)
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
    print_cmd_output(cmd)
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
    print_cmd_output(cmd)
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
    print_cmd_output(cmd)
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
