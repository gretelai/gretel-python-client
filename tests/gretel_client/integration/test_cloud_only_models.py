from typing import Callable

import docker
import pytest

from gretel_client.config import RunnerMode
from gretel_client.docker import AuthStrategy, DockerError, pull_image
from gretel_client.projects import Project
from gretel_client.projects.models import Model
from python.tests.gretel_client.integration.conftest import pytest_skip_on_windows


@pytest.mark.skip("Update and enable after PR#3072 gets merged")
def test_cannot_run_tabllm_locally(project: Project, get_fixture: Callable):
    model: Model = project.create_model_obj(get_fixture("tabllm_config.yml"))
    with pytest.raises(Exception):
        model.submit(runner_mode=RunnerMode.LOCAL)


@pytest.mark.skip("Update and enable after PR#3072 gets merged")
@pytest_skip_on_windows
def test_cannot_download_tabllm_image():
    docker_client = docker.from_env()
    with pytest.raises(DockerError) as e:
        pull_image(
            "074762682575.dkr.ecr.us-east-2.amazonaws.com/cloud-only-models/tabllm:dev",
            docker_client,
            auth_strategy=AuthStrategy.AUTH,
        )

    assert "Could not pull image" in str(e.value)
