import json
from pathlib import Path

from gretel_client_v2.projects import get_project, Project

PRE_TRAIN_PROJECT = "gretel-client-project-pretrained"

FIXTURES = Path(__file__).parent / "fixtures"


def build_synth_model(project: Project):
    ...


def build_xf_model(project: Project):
    ...


if __name__ == "__main__":
    project = get_project(name=PRE_TRAIN_PROJECT, create=True)

    print(json.dumps({
        "synthetics": build_synth_model(project),
        "transforms": build_xf_model(project)
    }))
