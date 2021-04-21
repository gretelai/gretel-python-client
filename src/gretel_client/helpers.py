import requests
import yaml


_url_template = "https://raw.githubusercontent.com/gretelai/gretel-blueprints/{branch}/config_templates/gretel/synthetics/{name}.yml"


def get_synthetics_config(config: str = "default", branch: str = "main") -> dict:
    """Given the name of a Gretel Synthetics configuration template, retrieve the
    full configuration file and extract just the synthetic model parameters
    and return them as a dictionary.
    """
    _url = _url_template.format(branch=branch, name=config)
    try:
        resp = requests.get(_url)
    except Exception as err:
        raise RuntimeError(f"Error connecting to GitHub to retrieve configuration: {str(err)}")

    if resp.status_code != 200:
        raise RuntimeError(f"Failed to retrieve config from GitHub with status code: {resp.status_code}, data: {resp.text}")

    config_dict = yaml.safe_load(resp.content)
    params = config_dict["models"][0]["synthetics"]["params"]

    return {k: v for k, v in params.items() if v is not None}
