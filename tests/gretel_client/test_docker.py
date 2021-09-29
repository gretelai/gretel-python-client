from pathlib import Path
from typing import Callable

import docker
import docker.errors
import pytest

from gretel_client.projects.docker import DataVolume, extract_container_path


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
