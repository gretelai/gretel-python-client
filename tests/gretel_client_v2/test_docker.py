from typing import Callable
from pathlib import Path

from gretel_client_v2.projects.docker import VolumeBuilder


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
