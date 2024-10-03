import json
import time

from typing import Callable

import docker
import docker.errors
import pytest

from gretel_client.docker import Container, PullProgressPrinter


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
        image="hello-world:latest",
        auth_strategy=None,
        params=[],
        remove=False,
        detach=True,
    )
    request.addfinalizer(c.stop)  # in case the test fails, cleanup the container
    c.start()
    while c.active:
        time.sleep(0.1)
    assert "Hello from Docker!" in c.get_logs().strip()
    c.stop()
    with pytest.raises(docker.errors.NotFound):
        docker.from_env().containers.get(c.run.id)
