from time import sleep
from typing import Callable

import pytest

from gretel_client_v2.projects.docker import ContainerRun, _get_container_auth
from gretel_client_v2.projects.models import Model
from gretel_client_v2.projects.projects import get_project


@pytest.fixture
def model(get_fixture: Callable):
    project = get_project(create=True)
    model = Model(
        project=project, model_config=get_fixture("transforms_config.yml")
    )
    model.submit()
    yield model
    model.delete()
    project.delete()


# mark: integration
def test_does_start_local_container(model: Model):
    run = ContainerRun(model)
    run.start()
    assert run.container_status in {"created", "running"}
    run.wait()
    model._poll_model()
    assert model.status == "completed"


# mark: integration
def test_does_cleanup(model: Model):
    run = ContainerRun(model)
    run.start()
    # this check isn't deterministic. the status might
    # be created or running. if we're in either one of
    # these statuses, it's a good indication things are
    # working as expected.
    assert run.container_status in {"created", "running"}
    sleep(3)
    run._cleanup()
    assert run.container_status in {"removing", "unknown"}


# mark: integration
def test_does_auth_registry():
    auth, reg = _get_container_auth()
    assert auth
    assert reg
