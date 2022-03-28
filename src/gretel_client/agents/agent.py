"""
Classes responsible for running local Gretel worker agents.
"""
from __future__ import annotations

import logging
import os
import threading

from dataclasses import asdict, dataclass
from typing import Callable, Dict, Generic, Iterator, List, Optional

from backports.cached_property import cached_property

from gretel_client.agents.drivers.driver import ComputeUnit, Driver
from gretel_client.agents.drivers.registry import get_driver
from gretel_client.agents.logger import configure_logging
from gretel_client.config import configure_custom_logger, get_session_config
from gretel_client.docker import CloudCreds, DataVolumeDef
from gretel_client.projects import get_project
from gretel_client.rest.apis import JobsApi, ProjectsApi, UsersApi


class AgentError(Exception):
    ...


@dataclass
class AgentConfig:
    """Provides various configuration knobs for running a Gretel Agent."""

    driver: str
    """Defines the driver used to launch containers from."""

    max_workers: int = 1
    """The max number of workers the agent instance will launch."""

    log_factory: Callable = lambda _: None
    """A factory function to ship worker logs to. If none is provided
    log messages from the worker will be suppressed, though they still
    be logged to their respective artifact endpoint.
    """

    project: Optional[str] = None
    """Determines the project to pull jobs for. If none if provided, than
    jobs from all projects will be fetched.
    """

    creds: Optional[List[CloudCreds]] = None
    """Provide credentials to propagate to the worker"""

    artifact_endpoint: Optional[str] = None
    """Configure an artifact endpoint for workers to store intermediate data on."""

    volumes: Optional[List[DataVolumeDef]] = None
    """A list of volumes to mount into the worker container"""

    env_vars: Optional[dict] = None
    """A list of environment variables to mount into the container"""

    _max_runtime_seconds: Optional[int] = None
    """TODO: implement"""

    def __post_init__(self):
        if not self._max_runtime_seconds:
            self._max_runtime_seconds = self._lookup_max_runtime()

    @property
    def max_runtime_seconds(self) -> int:
        if not self._max_runtime_seconds:
            raise AgentError("Could not fetch user config. Please restart the agent.")
        return self._max_runtime_seconds

    def _lookup_max_runtime(self) -> int:
        user_api = get_session_config().get_api(UsersApi)
        return (
            user_api.users_me()
            .get("data")
            .get("me")
            .get("service_limits")
            .get("max_job_runtime")
        )

    @property
    def as_dict(self) -> dict:
        return asdict(self)

    @cached_property
    def project_id(self) -> str:
        project = get_project(name=self.project)
        return project.project_id


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
    max_runtime_seconds: int
    log: Optional[Callable] = None
    cloud_creds: Optional[List[CloudCreds]] = None
    artifact_endpoint: Optional[str] = None
    env_vars: Optional[dict] = None

    @classmethod
    def from_dict(cls, source: dict, agent_config: AgentConfig) -> Job:
        return cls(
            uid=source["run_id"] or source["model_id"],
            job_type=source["job_type"],
            container_image=source["container_image"],
            worker_token=source["worker_token"],
            log=agent_config.log_factory(source["run_id"]),
            max_runtime_seconds=agent_config.max_runtime_seconds,
            cloud_creds=agent_config.creds,
            artifact_endpoint=agent_config.artifact_endpoint,
            env_vars=agent_config.env_vars,
        )

    @property
    def params(self) -> Dict[str, str]:
        params = {"--worker-token": self.worker_token}
        if self.artifact_endpoint:
            params["--artifact-endpoint"] = self.artifact_endpoint
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
    def gretel_stage(self) -> str:
        if "https://api-dev.gretel.cloud" in get_session_config().endpoint:
            return "dev"
        return "prod"


class RateLimiter:
    """Limits the amount of jobs the agent can place."""

    def __init__(self, max_active_jobs: int, job_manager: JobManager):
        self._job_manger = job_manager
        self._max_active_jobs = max_active_jobs

    def has_capacity(self) -> bool:
        return self._job_manger.active_jobs < self._max_active_jobs


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

    def add_job(self, job: Job, unit: ComputeUnit):
        self._active_jobs[job.uid] = unit

    def _update_active_jobs(self):
        for job in list(self._active_jobs):
            if not self._driver.active(self._active_jobs[job]):
                self._logger.info(f"Job {job} completed")
                self._driver.clean(self._active_jobs[job])
                self._active_jobs.pop(job)

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
        self, jobs_api: JobsApi, rate_limiter: RateLimiter, agent_config: AgentConfig
    ):
        self._agent_config = agent_config
        self._jobs_api = jobs_api
        self._rate_limiter = rate_limiter
        self._logger = logging.getLogger(__name__)
        self._interupt = threading.Event()

    def __iter__(self):
        return self

    def interupt(self):
        return self._interupt.set()

    def poll_endpoint(self) -> Optional[Job]:
        next_job = self._jobs_api.receive_one(project_id=self._agent_config.project_id)
        if next_job["data"]["job"] is not None:
            return Job.from_dict(next_job["data"]["job"], self._agent_config)

    def __next__(self) -> Optional[Job]:
        wait_secs = 2
        while True and not self._interupt.is_set():
            if self._rate_limiter.has_capacity():
                job = None
                try:
                    job = self.poll_endpoint()
                except Exception as ex:
                    self._logger.warning(
                        f"There was a problem calling the jobs endpoint {ex}"
                    )
                if job:
                    return job
            self._interupt.wait(wait_secs)
            if wait_secs > Poller.max_wait_secs:
                wait_secs = 2
                self._logger.info("Heartbeat from poller, still here...")
            else:
                wait_secs += wait_secs**2


class Agent:
    """Starts an agent"""

    def __init__(self, config: AgentConfig):
        configure_logging()
        self._logger = logging.getLogger(__name__)
        configure_custom_logger(self._logger)
        self._config = config
        self._client_config = get_session_config()
        self._driver = get_driver(config)
        self._jobs_manager = JobManager(self._driver)
        self._rate_limiter = RateLimiter(AgentConfig.max_workers, self._jobs_manager)
        self._jobs_api = self._client_config.get_api(JobsApi)
        self._projects_api = self._client_config.get_api(ProjectsApi)
        self._jobs = Poller(self._jobs_api, self._rate_limiter, self._config)
        self._interupt = threading.Event()

    def start(self, cooloff: float = 5):
        """Start the agent"""
        self._logger.info("Agent started, waiting for work to arrive")
        for job in self._jobs:
            if not job:
                if self._interupt.is_set():
                    return
                else:
                    continue
            self._logger.info(f"Got {job.job_type} job {job.uid}, scheduling now.")

            self._jobs_manager.add_job(job, self._driver.schedule(job))
            self._logger.info(f"Container for job {job.uid} scheduled")
            # todo: add in read lock to jobs endpoint. this sleep is
            # a stopgap until then. without this the agent is going to
            # try and launch multiple containers of the same job.
            self._interupt.wait(cooloff)

    def interupt(self):
        """Shuts down the agent"""
        self._jobs.interupt()
        self._interupt.set()
        self._logger.info("Server preparing to shutdown")
        self._jobs_manager.shutdown()
        self._logger.info("Server shutdown complete")


def get_agent(config: AgentConfig) -> Agent:
    return Agent(config=config)
