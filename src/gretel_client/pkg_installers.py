import selectors
import sys
import subprocess

import requests


PKG_ENDPOINT = "https://{}/opt/pkg"

TX_PKG = "gretel-transformers"


class GretelInstallError(Exception):
    pass


def read_pipe(section, cmd, p, verbose, collect=False):
    out = [
        f"\n{section}\n{''.join(['=' for _ in range(len(section)+1)])}",
        " ".join(cmd),
    ]
    if verbose and not collect:
        for line in out:
            print(line)
    for line in iter(p.readline, b""):
        if isinstance(line, bytes):
            line = line.decode("utf-8").strip()  # type: ignore
        if verbose and not collect:
            print(line)
        if collect:
            out.append(line)

    if verbose and collect and len(out) > 2:
        for line in out:
            print(line)
        print("")


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
        print(f"running: {' '.join(cmd)}")
    results = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    read_pipe("pip: Install Output", cmd, results.stdout, verbose)
    read_pipe("pip: Errors & Warnings", cmd, results.stderr, verbose=True, collect=True)


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


def install_transformers(api_key: str, host: str, verbose: bool = False):
    print("Authenticating with package manager")
    package_endpoint = _get_package_endpoint(TX_PKG, api_key, host)

    print("Installing packages (this might take a while)")
    _install_pip_dependency(package_endpoint, verbose)
    print("Finished installing Gretel packages")
