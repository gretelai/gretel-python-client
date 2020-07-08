import logging
import sys
import subprocess

import requests


PKG_ENDPOINT = "https://{}/opt/pkg"
TX_PKG = "gretel-transformers"


logger = logging.getLogger(__name__)
logging.basicConfig(format="%(levelname)s %(filename)s: %(message)s")
logger.setLevel(logging.INFO)


class GretelInstallError(Exception):
    pass


def read_pipe(cmd, p, verbose, log=logger.info, collect=False):
    out = [
        " ".join(cmd),
    ]
    if verbose and not collect:
        for line in out:
            log(line)
    for line in iter(p.readline, b""):
        if isinstance(line, bytes):
            line = line.decode("utf-8").strip()  # type: ignore
        if verbose and not collect:
            log(line)
        if collect:
            out.append(line)

    if verbose and collect and len(out) > 1:
        for line in out:
            log(line)
        log("")


def _install_pip_dependency(dep: str, verbose: bool = True):
    """Install pip dependency via a subprocess"""
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "--disable-pip-version-check",
        "install",
        dep,
    ]
    if verbose:
        logger.info(f"running: {' '.join(cmd)}")
    results = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    read_pipe(cmd, results.stdout, verbose)
    read_pipe(
        cmd, results.stderr, verbose=True, log=logger.error, collect=True,
    )


def _get_package_endpoint(package_identifier: str, api_key: str, host: str) -> str:
    """Retrieve package installation endpoints from Gretel API"""
    pkg_resp = requests.get(
        f"{PKG_ENDPOINT.format(host)}/{package_identifier}",
        headers={"Authorization": api_key},
    )

    if pkg_resp.status_code != 200:  # pragma: no cover
        raise GretelInstallError("Could not fetch package details from " "endpoint.")

    details = pkg_resp.json()
    source_pkg = details["data"]["package"]["wheel"]
    return source_pkg


def install_packages(api_key: str, host: str, verbose: bool = False):
    logger.info("Authenticating with package manager")
    package_endpoint = _get_package_endpoint(TX_PKG, api_key, host)

    logger.info("Installing packages (this might take a while)")
    _install_pip_dependency(package_endpoint, verbose)
    logger.info("Finished installing Gretel packages")
