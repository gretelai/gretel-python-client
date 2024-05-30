import logging

from pathlib import Path
from typing import Callable, List, Optional

import click

from gretel_client.agents.agent import AgentConfig, get_agent
from gretel_client.agents.drivers.driver import GPU
from gretel_client.cli.common import pass_session, SessionContext
from gretel_client.config import get_session_config, RunnerMode
from gretel_client.docker import AwsCredFile, CaCertFile, check_gpu, DataVolumeDef


@click.group(
    help="Connect Gretel with a data source.",
    hidden=not get_session_config().preview_features_enabled,
)
def agent(): ...


def build_logger(job_id: str) -> Callable:
    logger = logging.getLogger(f"job_{job_id}")
    return logger.info


@agent.command(help="Start Gretel worker agent.")
@click.option(
    "--driver",
    metavar="NAME",
    help="Specify driver used to launch new workers.",
    default="docker",
)
@click.option(
    "--max-workers", metavar="COUNT", help="Max number of workers to launch.", default=2
)
@click.option(
    "--project",
    allow_from_autoenv=True,
    envvar="GRETEL_DEFAULT_PROJECT",
    help="CSV of Gretel projects to execute command from.",
    metavar="NAME",
)
@click.option(
    "--same-org-only",
    is_flag=True,
    envvar="GRETEL_SAME_ORG_ONLY",
    allow_from_autoenv=True,
    help="If this is set, only jobs from the same organization as the running user will be executed.",
    type=bool,
)
@click.option(
    "--auto-accept-project-invites",
    is_flag=True,
    envvar="GRETEL_AGENT_AUTO_ACCEPT_PROJECT_INVITES",
    allow_from_autoenv=True,
    help="If this is set, the Gretel Agent will automatically check for and accept project invites originating from within the organization.",
    type=bool,
)
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
@click.option(
    "--runner-modes",
    metavar="RUNNER_MODES",
    help="Runner modes used to poll the jobs endpoint",
    default=None,
    envvar="RUNNER_MODES",
    multiple=True,
)
@click.option(
    "--env",
    metavar="KEY=VALUE",
    help="Pass environment variables into the worker container.",
    multiple=True,
)
@click.option(
    "--volume",
    metavar="HOST:CONTAINER",
    help="Mount single file into the worker container. HOST and CONTAINER must be files.",
    multiple=True,
)
@click.option(
    "--ca-bundle",
    metavar="PATH",
    help="Mount custom CA into each worker container.",
)
@click.option(
    "--disable-cloud-logging",
    help="Disable sending worker logs to Gretel Cloud.",
    default=False,
)
@click.option(
    "--disable-cloud-report-scores",
    help="Disable sending model report scores to Gretel Cloud.",
    default=False,
)
@click.option(
    "--enable-prometheus",
    help="Enable the prometheus metrics endpoint on port 8080",
    default=False,
)
@click.option(
    "--cluster-id",
    help="Hybrid Cluster ID to operate on",
    envvar="GRETEL_CLUSTER_ID",
)
@click.option(
    "--app-version",
    help="Version of the application running, if appropriate",
    envvar="GRETEL_APP_VERSION",
)
@click.option(
    "--image-version",
    help="Version of the image running, if appropriate",
    envvar="GRETEL_IMAGE_VERSION",
)
@click.option(
    "--disable-job-cleanup",
    help="Disables the cleanup of jobs to help debugging",
    envvar="GRETEL_DISABLE_JOB_CLEANUP",
    type=bool,
)
@pass_session
def start(
    sc: SessionContext,
    driver: str,
    max_workers: int,
    project: Optional[str] = None,
    same_org_only: bool = False,
    auto_accept_project_invites: bool = False,
    aws_cred_path: Optional[str] = None,
    artifact_endpoint: Optional[str] = None,
    env: Optional[List[str]] = None,
    volume: Optional[List[str]] = None,
    ca_bundle: Optional[str] = None,
    disable_cloud_logging: bool = False,
    disable_cloud_report_scores: bool = False,
    enable_prometheus: bool = False,
    runner_modes: Optional[List[str]] = None,
    cluster_id: Optional[str] = None,
    disable_job_cleanup: bool = False,
    app_version: Optional[str] = None,
    image_version: Optional[str] = False,
):
    sc.log.info(f"Starting Gretel agent using driver {driver}.")
    creds = []

    if aws_cred_path:
        creds.append(AwsCredFile(cred_from_agent=aws_cred_path))

    if ca_bundle:
        creds.append(CaCertFile(cred_from_agent=ca_bundle))

    volumes = []
    if volume:
        for vol in volume:
            host_path, target = vol.split(":", maxsplit=1)
            target_path = Path(target)
            volumes.append(
                DataVolumeDef(str(target_path.parent), [(host_path, target_path.name)])
            )

    env_dict = dict(e.split("=", maxsplit=1) for e in env) if env else None

    capabilities = []
    if driver == "docker":
        sc.log.info("Checking for GPU.")
        if check_gpu():
            capabilities.append(GPU)
            sc.log.info("GPU found.")
        else:
            sc.log.info("No GPU found. Continuing without one.")
    runner_modes_as_enum = None
    if runner_modes:
        runner_modes_as_enum = [
            RunnerMode.parse(runner_mode_str) for runner_mode_str in runner_modes
        ]
        if RunnerMode.CLOUD in runner_modes_as_enum:
            raise ValueError("'cloud' is not a legal runner mode for the agent")

    projects = []
    if project:
        projects = [p.strip() for p in project.split(",")]

    config = AgentConfig(
        projects=projects,
        org_only=same_org_only,
        auto_accept_project_invites=auto_accept_project_invites,
        max_workers=max_workers,
        driver=driver,
        creds=creds,
        log_factory=build_logger,
        artifact_endpoint=artifact_endpoint,
        disable_cloud_logging=disable_cloud_logging,
        disable_cloud_report_scores=disable_cloud_report_scores,
        env_vars=env_dict,
        volumes=volumes,
        capabilities=capabilities,
        enable_prometheus=enable_prometheus,
        runner_modes=runner_modes_as_enum,
        cluster_guid=cluster_id,
        session=sc.session,
        disable_job_cleanup=disable_job_cleanup,
        app_version=app_version,
        image_version=image_version,
    )
    agent = get_agent(config)
    sc.register_cleanup(lambda: agent.interrupt())
    agent.start()
