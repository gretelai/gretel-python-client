"""
Classes responsible for running local Gretel worker agents.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import threading

from collections import Counter
from dataclasses import asdict, dataclass, field
from threading import Thread
from time import sleep
from typing import Callable, Dict, Generic, Iterator, List, Optional, Tuple

import requests

import gretel_client.agents.agent_telemetry as telemetry

from gretel_client.agents.drivers.driver import ComputeUnit, Driver
from gretel_client.agents.drivers.registry import get_driver
from gretel_client.agents.logger import configure_logging
from gretel_client.config import configure_custom_logger, get_session_config, RunnerMode
from gretel_client.docker import CloudCreds, DataVolumeDef
from gretel_client.helpers import do_api_call
from gretel_client.projects import get_project
from gretel_client.projects.exceptions import GretelProjectError
from gretel_client.rest.apis import JobsApi, ProjectsApi

configure_logging()


class AgentError(Exception):
    ...


@dataclass
class AgentConfig:
    """Provides various configuration knobs for running a Gretel Agent."""

    driver: str
    """Defines the driver used to launch containers from."""

    max_workers: int = 1
    """The max number of workers the agent instance will launch."""

    project_retry_limit: int = 10
    """The max number of times to retry resolving a project."""

    _max_workers_from_config: int = 0
    """The max workers that were passed in via the config"""

    log_factory: Callable = lambda _: None
    """A factory function to ship worker logs to. If none is provided
    log messages from the worker will be suppressed, though they still
    be logged to their respective artifact endpoint.
    """

    projects: List[str] = field(default_factory=list)
    """Determines the projects to pull jobs for. If none is provided, then
    jobs from all projects will be fetched.
    """

    creds: Optional[List[CloudCreds]] = None
    """Provide credentials to propagate to the worker"""

    artifact_endpoint: Optional[str] = None
    """Configure an artifact endpoint for workers to store intermediate data on."""

    disable_cloud_logging: bool = False
    """Disable sending worker logs to the cloud"""

    volumes: Optional[List[DataVolumeDef]] = None
    """A list of volumes to mount into the worker container"""

    env_vars: Optional[dict] = None
    """A list of environment variables to mount into the container"""

    capabilities: Optional[List[str]] = None
    """A list of capabilities the agent has available for scheduling"""

    enable_prometheus: bool = False
    """Sets up the prometheus endpoint for the running agent"""

    runner_modes: Optional[List[RunnerMode]] = None

    _project_ids: Optional[List[str]] = None
    """Cached project ID."""

    _failed_projects: Counter = field(default_factory=Counter)
    """Projects that we have failed to resolve."""

    def __post_init__(self):
        self._logger = logging.getLogger(__name__)
        self._max_workers_from_config = self.max_workers
        self._update_max_workers()

    def _update_max_workers(self) -> int:
        initial_value = self.max_workers

        max_jobs_active = self._lookup_max_jobs_active()

        self.max_workers = (
            max_jobs_active
            if self._max_workers_from_config <= 0
            else min(max_jobs_active, self._max_workers_from_config)
        )

        if (
            self._max_workers_from_config > 0
            and max_jobs_active < self._max_workers_from_config
        ):
            self._logger.warning(
                "Max workers set by config (%d) higher than the value from the API (%d)",
                self._max_workers_from_config,
                max_jobs_active,
            )

        if initial_value != self.max_workers:
            self._logger.info(
                "Max workers set to %d",
                self.max_workers,
            )

        telemetry.set_max_workers(
            max_workers=self.max_workers, previous_max_workers=initial_value
        )
        return self.max_workers

    def _lookup_max_jobs_active(self) -> int:
        try:
            resp = do_api_call("GET", "/users/me/billing")
            return (
                resp.get("billing")
                .get("me")
                .get("service_limits")
                .get("max_jobs_active")
            )
        except Exception as ex:
            logger = logging.getLogger(__name__)
            logger.exception("")
            raise AgentError(
                "Error looking up service limits from the Gretel Cloud API. "
                "Please ensure your Gretel endpoint and API key are correct "
                "and that you have connectivity to Gretel Cloud."
            ) from ex

    @property
    def as_dict(self) -> dict:
        return asdict(self)

    @property
    def project_ids(self) -> List[str]:
        if not self.projects:
            return []
        self._check_cache()
        if not self._project_ids:
            # Need to throw in this scenario to avoid not filtering by at least one project.
            raise GretelProjectError("unable to resolve any supplied projects")
        return self._project_ids

    def invalidate_project_ids(self) -> None:
        self._project_ids = None

    def _check_cache(self):
        if self._project_ids and len(self._failed_projects) == 0:
            # Cache has all needed values.
            return

        # If cache is empty, resolve everything. Otherwise, just resolve previously failed projects.
        if not self._project_ids:
            self._project_ids = []
            name_id_pairs = self._resolve_project_ids(self.projects)
        else:
            name_id_pairs = self._resolve_project_ids(
                list(self._failed_projects.keys())
            )
        for project_name, project_id in name_id_pairs:
            if project_id:
                self._project_ids.append(project_id)
                self._failed_projects.pop(project_name, 0)
            else:
                self._failed_projects[project_name] += 1

        # Throw if values overrun the retry limit.
        failures_over_limit = [
            x
            for x, count in self._failed_projects.items()
            if count > self.project_retry_limit
        ]
        if len(failures_over_limit) > 0:
            raise GretelProjectError(
                "projects (%s) have exceeded the resolution retry limit",
                ", ".join(failures_over_limit),
            )

    def _resolve_project_ids(
        self, projects_to_resolve: List[str]
    ) -> List[Tuple[str, Optional[str]]]:
        resolved = []
        for project_name in projects_to_resolve:
            try:
                project = get_project(name=project_name)
                resolved.append((project_name, project.project_id))
            except GretelProjectError:
                resolved.append((project_name, None))
        return resolved


@dataclass
class Job:

    """Job container class.

    Contains various Gretel Job properties that are used by each
    driver to configure and run the respective job.
    """

    uid: str
    job_type: str
    container_image: str
    worker_token: str
    instance_type: str
    log: Optional[Callable] = None
    cloud_creds: Optional[List[CloudCreds]] = None
    artifact_endpoint: Optional[str] = None
    disable_cloud_logging: bool = False
    env_vars: Optional[dict] = None

    @classmethod
    def from_dict(cls, source: dict, agent_config: AgentConfig) -> Job:
        return cls(
            uid=source["run_id"] or source["model_id"],
            job_type=source["job_type"],
            instance_type=source["instance_type"],
            container_image=source["container_image"],
            worker_token=source["worker_token"],
            log=agent_config.log_factory(
                source.get("run_id") or source.get("model_id")
            ),
            cloud_creds=agent_config.creds,
            artifact_endpoint=agent_config.artifact_endpoint,
            disable_cloud_logging=agent_config.disable_cloud_logging,
            env_vars=agent_config.env_vars,
        )

    @property
    def params(self) -> Dict[str, str]:
        params = {"--worker-token": self.worker_token}
        if self.artifact_endpoint:
            params["--artifact-endpoint"] = self.artifact_endpoint
        if self.disable_cloud_logging:
            params["--disable-cloud-logging"] = ""
        return params

    @property
    def env(self) -> Dict[str, str]:
        params = {
            "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", ""),
            "GRETEL_STAGE": self.gretel_stage,
        }
        if self.cloud_creds:
            for cred in self.cloud_creds:
                params.update(cred.env)
        if self.env_vars:
            params.update(self.env_vars)
        return params

    @property
    def secret_env(self) -> Dict[str, str]:
        env = {"GRETEL_WORKER_TOKEN": self.worker_token}
        return env

    @property
    def gretel_stage(self) -> str:
        return get_session_config().stage

    @property
    def gretel_endpoint(self) -> str:
        return get_session_config().endpoint

    @property
    def needs_gpu(self) -> bool:
        return "gpu" in self.instance_type.lower()


class RateLimiter:
    """Limits the amount of jobs the agent can place."""

    def __init__(
        self, max_active_jobs: int, job_manager: JobManager, agent_config: AgentConfig
    ):
        self._job_manger = job_manager
        self._max_active_jobs = max_active_jobs
        self._agent_config = agent_config
        self._logger = logging.getLogger(__name__)
        self._poll_interval_secs = 300

    def has_capacity(self) -> bool:
        return self._job_manger.active_jobs < self._max_active_jobs

    def update_max_active_jobs(self):
        previous = self._max_active_jobs
        new_value = self._agent_config._update_max_workers()
        if previous != new_value:
            self._max_active_jobs = new_value

    def update_max_active_jobs_loop(self):
        while True:
            try:
                self.update_max_active_jobs()
                sleep(self._poll_interval_secs)
            except Exception:
                self._logger.warning(
                    "Error checking max active jobs endpoints", exc_info=True
                )


class JobManager(Generic[ComputeUnit]):
    """Responsible for tracking the status of jobs as they are
    scheduled and compelted.

    TODO: Add support for cleaning stuck jobs based on a config's
        max runtime.

    """

    def __init__(self, driver: Driver):
        self._active_jobs: Dict[str, ComputeUnit] = {}
        self._driver = driver
        self._logger = logging.getLogger(__name__)

    def add_job(self, job: Job, unit: ComputeUnit) -> None:
        self._active_jobs[job.uid] = unit
        telemetry.increase_active_jobs()

    def _update_active_jobs(self) -> None:
        for job in list(self._active_jobs):
            if not self._driver.active(self._active_jobs[job]):
                self._logger.info("Job %s completed/ended", job)
                self._driver.clean(self._active_jobs[job])
                self._active_jobs.pop(job)
                telemetry.decrease_active_jobs()

    def contains_job(self, job: Job) -> bool:
        return job.uid in self._active_jobs

    @property
    def active_jobs(self) -> int:
        self._update_active_jobs()
        return len(self._active_jobs)

    def shutdown(self):
        self._logger.info("Attemping to shutdown job manager")
        self._update_active_jobs()
        for job, unit in self._active_jobs.items():
            self._logger.info(f"Shutting down job {job} unit {unit}")
            self._driver.shutdown(unit)


class Poller(Iterator):
    """
    Provides an iterator interface for fetching polling for new
    ``Job``s from the API. If no jobs are available, the iterator
    will block until a new ``Job`` is available.

    Args:
        jobs_api: Job api client instance.
        rate_limiter: Uses to ensure new jobs aren't returned until the
            agent has capacity.
        agent_config: Agent config used to configure the ``Job``.

    """

    max_wait_secs = 16

    def __init__(
        self,
        jobs_api: JobsApi,
        rate_limiter: RateLimiter,
        agent_config: AgentConfig,
    ):
        self._agent_config = agent_config
        self._jobs_api = jobs_api
        self._rate_limiter = rate_limiter
        self._logger = logging.getLogger(__name__)
        self._interrupt = threading.Event()
        self._runner_modes = agent_config.runner_modes or [RunnerMode.MANUAL]

    def __iter__(self):
        return self

    def interrupt(self):
        return self._interrupt.set()

    def poll_endpoint(self) -> Optional[Job]:
        api_kwargs = {}
        project_ids = self._agent_config.project_ids
        if len(project_ids) > 0:
            api_kwargs["project_ids"] = project_ids
        if self._runner_modes:
            api_kwargs["runner_modes"] = [
                runner_mode.value for runner_mode in self._runner_modes
            ]
        next_job = self._jobs_api.receive_one(**api_kwargs)
        if next_job["data"]["job"] is not None:
            return Job.from_dict(next_job["data"]["job"], self._agent_config)

    def __next__(self) -> Optional[Job]:
        wait_secs = 2
        while True and not self._interrupt.is_set():
            if self._rate_limiter.has_capacity():
                job = None
                try:
                    job = self.poll_endpoint()
                except Exception:
                    self._logger.warning(
                        "There was a problem calling the jobs endpoint.",
                        exc_info=True,
                    )
                    self._agent_config.invalidate_project_ids()
                if job:
                    return job
            self._interrupt.wait(wait_secs)
            if wait_secs > Poller.max_wait_secs:
                wait_secs = 2
                self._logger.info("Heartbeat from poller, still here...")
            else:
                wait_secs += wait_secs**2


class Agent:
    """Starts an agent"""

    def __init__(self, config: AgentConfig):
        self._logger = logging.getLogger(__name__)
        configure_custom_logger(self._logger)
        self._config = config
        self._client_config = get_session_config()
        self._driver = get_driver(config)
        self._jobs_manager = JobManager(self._driver)
        self._rate_limiter = RateLimiter(config.max_workers, self._jobs_manager, config)
        self._jobs_api = self._client_config.get_api(JobsApi)
        self._projects_api = self._client_config.get_api(ProjectsApi)
        self._jobs = Poller(
            self._jobs_api,
            self._rate_limiter,
            self._config,
        )
        self._interrupt = threading.Event()

    def start(self):
        """Start the agent"""
        if self._config.enable_prometheus:
            self._logger.info("Enabling prometheus client")
            telemetry.setup_prometheus()
            telemetry.set_max_workers(
                max_workers=self._config.max_workers,
            )
        self._logger.info("Agent started, waiting for work to arrive")

        thread = Thread(
            target=self._rate_limiter.update_max_active_jobs_loop, daemon=True
        )
        thread.start()

        for job in self._jobs:
            if not job:
                if self._interrupt.is_set():
                    return
                else:
                    continue
            if self._jobs_manager.contains_job(job):
                self._logger.info(f"Job {job.uid} already in process, skipping")
                continue
            self._logger.info(f"Got {job.job_type} job {job.uid}, scheduling now.")
            unit = self._driver.schedule(job)

            if not unit:
                self._logger.warning(f"Unable to schedule job {job.uid}")
                telemetry.increment_job_count(error=True)
            else:
                self._jobs_manager.add_job(job, unit)
                self._logger.info(f"Container for job {job.uid} scheduled")
                telemetry.increment_job_count()
                self._update_job_status(job)

    def _update_job_status(self, job: Job) -> None:
        try:
            self._logger.info("Updating status to Pending for job %s", job.uid)
            worker_json = base64.standard_b64decode(job.worker_token).decode("ascii")
            worker_key = json.loads(worker_json)["model_key"]
            headers = {"Authorization": worker_key}
            url = f"{job.gretel_endpoint}/projects/models"
            params = {"uid": job.uid, "type": job.job_type}
            data = {"uid": job.uid, "status": "pending"}
            self._logger.debug(url, headers, params, data)
            resp = requests.patch(
                url,
                headers=headers,
                json=data,
                params=params,
                timeout=15,
            )
            self._logger.debug(resp.text)
            resp.raise_for_status()
        except Exception as ex:
            self._logger.error("There was an error updating the job status: %s", ex)

    def interrupt(self):
        """Shuts down the agent"""
        self._jobs.interrupt()
        self._interrupt.set()
        self._logger.info("Server preparing to shutdown")
        self._jobs_manager.shutdown()
        self._logger.info("Server shutdown complete")


def get_agent(config: AgentConfig) -> Agent:
    return Agent(config=config)
