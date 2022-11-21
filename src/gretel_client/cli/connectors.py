import pathlib

import click
import yaml

from gretel_client.cli.common import pass_session, project_option, SessionContext
from gretel_client.config import get_session_config
from gretel_client.docker import (
    AuthStrategy,
    AwsCredFile,
    build_container,
    DataVolumeDef,
)

CONNECTOR_CONTAINER = "gretelai/connector"
CONTAINER_CONFIG_PATH = "/etc/gretel/"
CONNECTOR_CONFIG = "connector.yaml"


@click.group(
    help="Connect Gretel with a data source.",
    hidden=not get_session_config().preview_features_enabled,
)
def connectors():
    ...


@connectors.command(help="Start a connector.")
@click.option(
    "--config", metavar="PATH", help="Path to connector config.", required=True
)
@click.option(
    "--model",
    metavar="ID",
    help="Reference to a local model or cloud model id.",
    envvar="GRETEL_MODEL",
    required=False,
)
@click.option(
    "--aws-cred-path",
    metavar="PATH",
    help=(
        "Path to AWS credential file. These will be copied to the connector "
        "container via a data volume."
    ),
)
@click.option(
    "--connector",
    metavar="NAME",
    help="Connector pipeline to start.",
    default="default",
)
@click.option(
    "--artifact-endpoint",
    metavar="ENDPOINT",
    help="Path to artifact endpoint. If none is provided Gretel Cloud will be used.",
    default=None,
    envvar="GRETEL_ARTIFACT_ENDPOINT",
)
@project_option
@pass_session
def start(
    sc: SessionContext,
    config: str,
    model: str,
    project: str,
    connector: str,
    aws_cred_path: str = None,
    artifact_endpoint: str = None,
):
    volumes = []
    env = {"GRETEL_API_KEY": sc.config.api_key, "GRETEL_ENDPOINT": sc.config.endpoint}
    sc.log.info("Configuring connector.")

    config_volume = DataVolumeDef(
        CONTAINER_CONFIG_PATH, [(pathlib.Path(config), CONNECTOR_CONFIG)]
    )
    volumes.append(config_volume)
    if aws_cred_path:
        sc.log.info(f"Copying local aws credentials {aws_cred_path} to container.")
        cred = AwsCredFile(cred_from_agent=aws_cred_path)
        volumes.append(cred.volume)
        env.update(cred.env)

    params = {
        "--project": project,
        "--model": model,
        "--config": f"{CONTAINER_CONFIG_PATH}/{CONNECTOR_CONFIG}",
    }

    if artifact_endpoint:
        params["--artifact-endpoint"] = artifact_endpoint

    container = build_container(
        image=parse_connector_version(config, connector),
        auth_strategy=AuthStrategy.AUTH_AND_RESOLVE,
        params=params,
        volumes=volumes,
        env=env,
        detach=True,
    )

    sc.register_cleanup(lambda: container.stop())

    sc.log.info("Starting connector container.")
    container.start()

    for log in container.logs():
        sc.print(data=log)


def parse_connector_version(config_path: str, connector: str) -> str:
    with open(config_path, "r") as fh:
        config = yaml.safe_load(fh)
    release = "latest"
    for config in config["connectors"]:
        if config.get("name") == connector:
            release = config.get("version", "latest")
    return f"{CONNECTOR_CONTAINER}:{release}"
