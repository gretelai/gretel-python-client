from typing import Callable

import docker
import pytest

from gretel_client.config import RunnerMode
from gretel_client.docker import AuthStrategy, DockerError, pull_image
from gretel_client.projects import Project
from gretel_client.projects.models import Model

from .conftest import pytest_skip_on_windows


def test_cannot_run_navigator_locally(project: Project, get_fixture: Callable):
    model: Model = project.create_model_obj(get_fixture("navigator_config.yml"))
    with pytest.raises(Exception):
        model.submit(runner_mode=RunnerMode.LOCAL)


@pytest_skip_on_windows
def test_cannot_download_navigator_image():
    docker_client = docker.from_env()
    with pytest.raises(DockerError) as e:
        pull_image(
            "074762682575.dkr.ecr.us-east-2.amazonaws.com/cloud-only-models/navigator:dev",
            docker_client,
            auth_strategy=AuthStrategy.AUTH,
        )

    assert "Could not pull image" in str(e.value)
