import json
import time

from pathlib import Path
from typing import Callable

import docker
import docker.errors
import pytest

from gretel_client.docker import (
    Container,
    DataVolume,
    extract_container_path,
    PullProgressPrinter,
)


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


# to see the progress bar, run
#   pytest -s test_docker.py::test_docker_pull_progress
def test_docker_pull_progress(get_fixture: Callable):
    update_fixture = get_fixture("docker_pull_progress.json")
    update_mock = iter(
        json.loads(line) for line in update_fixture.read_text().strip().split("\n")
    )
    progress_printer = PullProgressPrinter(update_mock)
    progress_printer.start()
    # assert that all progress updates have been handled
    with pytest.raises(StopIteration):
        next(update_mock)


def test_container(request):
    c = Container(
        image="busybox:latest",
        auth_strategy=None,
        params=["echo", "hello"],
        remove=False,
        detach=True,
    )
    request.addfinalizer(c.stop)  # in case the test fails, cleanup the container
    c.start()
    while c.active:
        time.sleep(1)
    assert c.get_logs().strip() == "hello"
    c.stop()
    with pytest.raises(docker.errors.NotFound):
        docker.from_env().containers.get(c.run.id)
