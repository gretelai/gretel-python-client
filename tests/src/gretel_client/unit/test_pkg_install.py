import io
from dataclasses import dataclass
from unittest.mock import patch

import pytest

from gretel_client import pkg_installers


@dataclass
class OptResponse:
    status_code: int

    def json(self):
        return {"data": {"package": {"wheel": "https://path/to/package.whl"}}}


@dataclass
class ProcessResponse:
    @property
    def stdout(self):
        return io.BytesIO(b"")

    @property
    def stderr(self):
        return io.BytesIO(b"")


@patch.object(pkg_installers.requests, "get")
@patch.object(pkg_installers.subprocess, "Popen")
def test_does_install_packages(popen, get_package):
    popen.return_value = ProcessResponse()
    get_package.return_value = OptResponse(status_code=200)
    pkg_installers.install_packages("test123", "api-dev.gretel.cloud")

    assert popen.call_args[0][0][-4:] == [
        "pip",
        "--disable-pip-version-check",
        "install",
        "https://path/to/package.whl",
    ]

    get_package.assert_called_with(
        "https://api-dev.gretel.cloud/opt/pkg/gretel-transformers",
        headers={"Authorization": "test123"},
    )


def test_requires_api_key():
    with pytest.raises(TypeError):
        pkg_installers.install_packages()  # type: ignore


@patch.object(pkg_installers.requests, "get")
def test_handles_api_down(get_package):
    get_package.return_value = OptResponse(status_code=500)

    with pytest.raises(pkg_installers.GretelInstallError):
        pkg_installers.install_packages("test123", "api-dev")
