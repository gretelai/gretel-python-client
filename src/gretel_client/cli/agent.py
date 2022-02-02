import logging

from typing import Callable

import click

from gretel_client.agents.agent import AgentConfig, get_agent
from gretel_client.cli.common import pass_session, project_option, SessionContext
from gretel_client.config import get_session_config
from gretel_client.docker import AwsCredFile


@click.group(
    help="Connect Gretel with a data source",
    hidden=not get_session_config().preview_features_enabled,
)
def agent():
    ...


def build_logger(job_id: str) -> Callable:
    logger = logging.getLogger(f"job_{job_id}")
    return logger.info


@agent.command(help="Start Gretel worker agent")
@click.option(
    "--driver",
    metavar="NAME",
    help="Specify driver used to launch new workers.",
    default="docker",
)
@click.option(
    "--max-workers", metavar="COUNT", help="Max number of workers to launch", default=2
)
@project_option
@click.option(
    "--aws-cred-path",
    metavar="PATH",
    help="Path to AWS credential file. These will be propagated to each worker.",
)
@click.option(
    "--artifact-endpoint",
    metavar="ENDPOINT",
    help="Path to artifact endpoint. If none is provided Gretel Cloud will be used.",
    default=None,
    envvar="GRETEL_ARTIFACT_ENDPOINT",
)
@pass_session
def start(
    sc: SessionContext,
    driver: str,
    max_workers: int,
    project: str = None,
    aws_cred_path: str = None,
    artifact_endpoint: str = None,
):
    sc.log.info(f"Starting Gretel agent using driver {driver}")
    aws_creds = AwsCredFile(cred_from_agent=aws_cred_path) if aws_cred_path else None
    config = AgentConfig(
        project=project,
        max_workers=max_workers,
        driver=driver,
        creds=aws_creds,
        log_factory=build_logger,
        artifact_endpoint=artifact_endpoint,
    )
    agent = get_agent(config)
    sc.register_cleanup(lambda: agent.interupt())
    agent.start()
