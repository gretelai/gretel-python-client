from typing import Callable

from click.testing import CliRunner

from gretel_client.cli.cli import cli
from gretel_client.projects.projects import Project


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
