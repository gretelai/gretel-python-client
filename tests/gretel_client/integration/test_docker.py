import tempfile

from pathlib import Path
from time import sleep
from typing import Callable

import docker
import pytest
import smart_open

from gretel_client.docker import DataVolume, extract_container_path, get_container_auth
from gretel_client.helpers import submit_docker_local
from gretel_client.projects.docker import ContainerRun
from gretel_client.projects.jobs import Status
from gretel_client.projects.models import Model
from gretel_client.projects.projects import get_project

from .conftest import pytest_skip_on_windows


@pytest.fixture
def model(get_fixture: Callable, request):
    project = get_project(create=True)
    request.addfinalizer(project.delete)
    model = Model(project=project, model_config=get_fixture("transforms_config.yml"))
    return model


@pytest_skip_on_windows
def test_does_start_local_container(model: Model):
    model.submit_local()
    run = ContainerRun.from_job(model)
    run.enable_cloud_uploads()
    run.start()
    assert run.container.container_status in {"created", "running"}
    # The default timeout is 30 seconds, but we need to give sufficient time
    # to pull the transforms image
    run.wait(180)
    model._poll_job_endpoint()
    assert model.status == Status.COMPLETED


@pytest_skip_on_windows
def test_does_cleanup(model: Model, get_fixture: Callable):
    model.submit_local()
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


@pytest_skip_on_windows
def test_does_auth_registry():
    auth, reg = get_container_auth()
    assert auth
    assert reg


@pytest_skip_on_windows
def test_data_volume(tmpdir: Path, get_fixture: Callable):
    client = docker.from_env()
    volume = DataVolume("/in", client)
    files_to_add = ["account-balances.csv", "xf_model.tar.gz"]
    for file in files_to_add:
        path = volume.add_file(get_fixture(file))
        assert path == f"/in/{file}"
    config = volume.prepare_volume()
    extract_container_path(volume.volume_container, "/in", str(tmpdir))  # type:ignore
    actual_files = [Path(file).name for file in tmpdir.listdir()]
    assert set(actual_files) == set(files_to_add)
    assert config[volume.name] == {"bind": "/in", "mode": "rw"}

    # check the file contents made it through
    assert set([Path(get_fixture(f)).stat().st_size for f in files_to_add]) == set(
        [Path(file).stat().st_size for file in tmpdir.listdir()]
    )

    volume.cleanup()

    with pytest.raises(docker.errors.NotFound):
        client.volumes.get(volume.volume.id)

    with pytest.raises(docker.errors.NotFound):
        client.containers.get(volume.volume_container.id)


@pytest_skip_on_windows
def test_does_run_record_handler(model: Model, get_fixture: Callable, tmpdir: Path):
    # NOTE: this test sends ref data as a local file into the worker, just to
    # test the plumbing
    in_data = get_fixture("account-balances.csv")
    with tempfile.NamedTemporaryFile() as temp_ref_data:
        with smart_open.open(in_data) as src_fin:
            temp_ref_data.write(src_fin.read().encode())
        temp_ref_data.seek(0)

        submit_docker_local(
            model,
            output_dir=tmpdir,
            in_data=in_data,
            ref_data={"foo": temp_ref_data.name},
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
            ref_data={"foo": temp_ref_data.name},
        )
        record_handler._poll_job_endpoint()
        assert record_handler.status == Status.COMPLETED
        assert (tmpdir / "data.gz").exists()
