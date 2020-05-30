import sys
import subprocess

import requests


PKG_ENDPOINT = 'https://{}/opt/pkg'

TX_PKG = 'gretel-transformers'


class GretelInstallError(Exception):
    pass


def _install_pip_dependency(dep: str):
    """Install pip dependency via a subprocess"""
    cmd = [sys.executable, "-m", "pip", "install", dep]
    print(f"running: {' '.join(cmd)}")
    results = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    for line in iter(results.stdout.readline, b''):  # pragma: no cover
        if isinstance(line, bytes):
            print(line.decode('utf-8').strip())  # type: ignore
        else:
            print(line)


def _get_package_endpoint(package_identifier: str, api_key: str, host: str) -> str:
    """Retrieve package installation endpoints from Gretel API"""
    pkg_resp = requests.get(f'{PKG_ENDPOINT.format(host)}/{package_identifier}',
                            headers={'Authorization': api_key})

    if pkg_resp.status_code != 200:  # pragma: no cover
        raise GretelInstallError('Could not fetch package details from '
                                 'endpoint.')

    details = pkg_resp.json()
    source_pkg = details['data']['package']['wheel']
    return source_pkg


def install_transformers(api_key: str, host: str):
    package_endpoint = _get_package_endpoint(TX_PKG, api_key, host)
    _install_pip_dependency(package_endpoint)
