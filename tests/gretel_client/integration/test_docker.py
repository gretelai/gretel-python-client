from pathlib import Path
from time import sleep
from typing import Callable

import pytest

from gretel_client.docker import get_container_auth
from gretel_client.helpers import submit_docker_local
from gretel_client.projects.docker import ContainerRun
from gretel_client.projects.jobs import Status
from gretel_client.projects.models import Model
from gretel_client.projects.projects import get_project


@pytest.fixture
def model(get_fixture: Callable, request):
    project = get_project(create=True)
    request.addfinalizer(project.delete)
    model = Model(project=project, model_config=get_fixture("transforms_config.yml"))
    return model


def test_does_start_local_container(model: Model):
    model.submit()
    run = ContainerRun.from_job(model)
    run.enable_cloud_uploads()
    run.start()
    assert run.container.container_status in {"created", "running"}
    run.wait()
    model._poll_job_endpoint()
    assert model.status == Status.COMPLETED


def test_does_cleanup(model: Model, get_fixture: Callable):
    model.submit()
    run = ContainerRun.from_job(model)
    run.enable_cloud_uploads()
    run.configure_input_data(get_fixture("account-balances.csv"))
    run.start()
    # this check isn't deterministic. the status might
    # be created or running. if we're in either one of
    # these statuses, it's a good indication things are
    # working as expected.
    assert run.container.container_status in {"created", "running", "completed"}
    run.graceful_shutdown()

    def check():
        return run.container.container_status in {"removing", "unknown", "exited"}

    wait = 0
    while wait < 20:
        if check():
            break
        else:
            wait += 1
            sleep(1)

    assert check


def test_does_auth_registry():
    auth, reg = get_container_auth()
    assert auth
    assert reg


def test_does_run_record_handler(model: Model, get_fixture: Callable, tmpdir: Path):
    submit_docker_local(
        model, output_dir=tmpdir, in_data=get_fixture("account-balances.csv")
    )
    model._poll_job_endpoint()
    assert model.status == Status.COMPLETED
    record_handler = model.create_record_handler_obj(
        data_source=str(get_fixture("account-balances.csv"))
    )
    submit_docker_local(
        record_handler,
        output_dir=tmpdir,
        model_path=(tmpdir / "model.tar.gz"),
    )
    record_handler._poll_job_endpoint()
    assert record_handler.status == Status.COMPLETED
    assert (tmpdir / "data.gz").exists()
