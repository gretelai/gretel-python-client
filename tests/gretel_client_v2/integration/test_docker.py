from time import sleep
from typing import Callable

import pytest

from gretel_client_v2.projects.docker import ContainerRun, _get_container_auth
from gretel_client_v2.projects.jobs import Status
from gretel_client_v2.projects.models import Model
from gretel_client_v2.projects.projects import get_project


@pytest.fixture
def model(get_fixture: Callable, request):
    project = get_project(create=True)
    request.addfinalizer(project.delete)
    model = Model(project=project, model_config=get_fixture("transforms_config.yml"))
    model.create()
    return model


def test_does_start_local_container(model: Model):
    run = ContainerRun.from_job(model)
    run.enable_cloud_uploads()
    run.start()
    assert run.container_status in {"created", "running"}
    run.wait()
    model._poll_job_endpoint()
    assert model.status == Status.COMPLETED


def test_does_cleanup(model: Model, get_fixture: Callable):
    run = ContainerRun.from_job(model)
    run.enable_cloud_uploads()
    run.configure_input_data(get_fixture("account-balances.csv"))
    run.start()
    # this check isn't deterministic. the status might
    # be created or running. if we're in either one of
    # these statuses, it's a good indication things are
    # working as expected.
    assert run.container_status in {"created", "running", "completed"}
    sleep(3)
    run._cleanup()
    assert run.container_status in {"removing", "unknown", "exited"}


def test_does_auth_registry():
    auth, reg = _get_container_auth()
    assert auth
    assert reg
