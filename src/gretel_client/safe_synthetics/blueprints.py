from pathlib import Path

import requests
import yaml

from smart_open import open

BASE_BLUEPRINT_REPO = "https://raw.githubusercontent.com/gretelai/gretel-blueprints/refs/heads/main/config_templates/gretel/tasks"


class TaskConfigError(Exception): ...


def load_blueprint_or_config(blueprint_or_config: dict | str) -> dict:
    """
    Try and load either a blueprint or config. Valid identifiers include
        - transform/default
        - my-transform-config.yaml
        - {...} (python dictionaries)
        - valid yaml strings

    """
    # if the input is a dict, we already know it's a concrete
    # config and can return it
    if isinstance(blueprint_or_config, dict):
        return blueprint_or_config

    try:
        blueprint_or_config = yaml.safe_load(blueprint_or_config)
    except yaml.YAMLError:
        pass

    # if attempting to parse the string into a dictionary worked,
    # then we got a yaml string as input, and now have a dictionary
    # to return
    if isinstance(blueprint_or_config, dict):
        return blueprint_or_config

    # if we can resolve the identifier to local
    # path, then try and load that
    if Path(blueprint_or_config).exists():
        return yaml.safe_load(Path(blueprint_or_config).read_text())

    try:
        return resolve_task_blueprint(blueprint_or_config)["task"]["config"]
    except TaskConfigError:
        pass

    raise TaskConfigError(
        f"Could not load config for: {blueprint_or_config}. "
        "We tried loading it as a dictionary, a local yaml file and then as "
        "a remote blueprint, but no luck."
    )


def resolve_task_blueprint(blueprint_id: str) -> dict:
    """
    Given a task config blueprint id, resolve it to a concrete
    path in the gretel-blueprints repository.
    """

    # blueprint_ids take the form of tab_ft/default
    # config paths take the form of [task_type]__[blueprint_name].yaml
    config_path = blueprint_id.replace("/", "__")

    path = f"{BASE_BLUEPRINT_REPO}/{config_path}.yaml"
    try:
        with open(path) as tpl:  # type:ignore
            tpl_str = tpl.read()
            return yaml.safe_load(tpl_str)
    except requests.exceptions.HTTPError as ex:
        raise TaskConfigError(
            f"Could not find or read the blueprint {config_path}"
        ) from ex
