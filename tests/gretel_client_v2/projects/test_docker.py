from pathlib import Path
from time import sleep
from typing import Callable

import pytest

from gretel_client_v2.projects.docker import (
    ContainerRun,
    VolumeBuilder,
    _get_container_auth,
)
from gretel_client_v2.projects.models import Model
from gretel_client_v2.projects.projects import get_project


@pytest.fixture
def model(get_fixture: Callable):
    project = get_project(create=True)
    model = Model(project=project, model_config=get_fixture("transforms_config.yml"))
    model.submit()
    yield model
    model.delete()
    project.delete()


# mark: integration
def test_does_start_local_container(model: Model, get_fixture: Callable):
    run = ContainerRun.from_model(model)
    run.enable_cloud_uploads()
    run.start()
    assert run.container_status in {"created", "running"}
    run.wait()
    model._poll_model()
    assert model.status == "completed"


# mark: integration
def test_does_cleanup(model: Model, get_fixture: Callable):
    run = ContainerRun.from_model(model)
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
    assert run.container_status in {"removing", "unknown"}


# mark: integration
def test_does_auth_registry():
    auth, reg = _get_container_auth()
    assert auth
    assert reg


def test_volume_configs(tmpdir: Path, get_fixture: Callable):
    volume = VolumeBuilder()
    volume.add_output_volume(str(tmpdir), "/tmp")
    volume.add_output_volume(None, "test")
    volume.add_input_volume(
        "/workspace",
        [
            ("in_data", str(get_fixture("account-balances.csv"))),
            ("model", str(get_fixture("xf_model.tar.gz"))),
            ("test", None),
        ],
    )

    assert {v["bind"] for v in volume.volumes.values()} == {"/tmp", "/workspace"}
    assert volume.input_mappings["in_data"] == "/workspace/account-balances.csv"
    assert volume.input_mappings["model"] == "/workspace/xf_model.tar.gz"
    assert "test" not in volume.input_mappings
