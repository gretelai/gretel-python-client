from __future__ import annotations
import base64
import json
import time
from abc import ABC, abstractmethod, abstractproperty
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Callable, Iterator, List, Optional, Tuple, Union

import smart_open
from gretel_client.config import DEFAULT_RUNNER, RunnerMode

from gretel_client.projects.common import (
    WAIT_UNTIL_DONE,
    ModelType,
    f,
    peek_classification_report,
    peek_synthetics_report,
    peek_transforms_report,
)
from gretel_client.rest.api.projects_api import ProjectsApi

if TYPE_CHECKING:
    from gretel_client.projects import Project
else:
    Project = None


@dataclass
class LogStatus:
    status: str
    transitioned: bool = False
    logs: List[dict] = field(default_factory=list)
    error: Optional[str] = None


class Status(str, Enum):
    CREATED = "created"
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    ERROR = "error"
    LOST = "lost"


ACTIVE_STATES = [Status.CREATED, Status.ACTIVE, Status.PENDING]
END_STATES = [Status.COMPLETED, Status.CANCELLED, Status.ERROR, Status.LOST]

CPU = "cpu"
GPU = "gpu"


class WaitTimeExceeded(Exception):
    """
    Thrown when the wait time specified by the user has expired.
    """

    ...


class Job(ABC):
    """Represents a unit of work that can be launched via
    a Gretel Worker.
    """

    project: Project
    """Project associated with the job"""

    worker_key: Optional[str]
    """Worker key used to launch the job"""

    _projects_api: ProjectsApi

    _data: Optional[dict] = None

    def __init__(
        self,
        project: Project,
        job_type: str,
        job_id: Optional[str],
    ):
        self.project = project
        self._projects_api = project.projects_api
        self.job_type = job_type
        self._data_key = job_type
        self._job_id = job_id
        if self._job_id:
            self._poll_job_endpoint()
        self._logs_iter_index = 0

    def submit(
        self, runner_mode: Union[str, RunnerMode] = DEFAULT_RUNNER, **kwargs
    ) -> dict:
        # todo: deprecate in favor of submit_manual and submit_cloud
        # Support `runner_mode` input as a str to provide
        # parity with the CLI semantics
        if isinstance(runner_mode, str):
            try:
                runner_mode = RunnerMode(runner_mode)
            except ValueError:
                raise ValueError(f"Invalid runner_mode: {runner_mode}")

        return self._submit(runner_mode, **kwargs).print_obj

    def submit_manual(self) -> Job:
        """Submit this Job to the Gretel Cloud API, which will create
        the job metadata but no runner will be started. The ``Model`` instance
        can now be passed into a dedicated runner.

        Returns:
            The response from the Gretel API.
        """
        return self._submit(runner_mode=RunnerMode.MANUAL)

    def submit_cloud(self) -> Job:
        """Submit this model to be scheduled for runing in Gretel Cloud.

        Returns:
            The response from the Gretel API.
        """
        if self.data_source:
            self.upload_data_source()
        return self._submit(runner_mode=RunnerMode.CLOUD)

    @abstractmethod
    def _submit(self, runner_mode: RunnerMode, **kwargs) -> Job:
        ...

    @abstractproperty
    def model_type(self) -> ModelType:
        ...

    @abstractmethod
    def _do_get_job_details(self) -> dict:
        ...

    @abstractmethod
    def _do_cancel_job(self):
        ...

    @abstractmethod
    def delete(self):
        ...

    @abstractproperty
    def instance_type(self):
        ...

    @abstractproperty
    def artifact_types(self) -> List[str]:
        ...

    @abstractmethod
    def _do_get_artifact(self, artifact_key: str) -> str:
        ...

    # Base Job properties

    @property
    def logs(self):
        """Returns run logs for the job."""
        return self._data.get(f.LOGS)

    @property
    def status(self) -> Status:
        """The status of the job. Is one of ``gretel_client.projects.jobs.Status``"""
        return Status(self._data[self.job_type][f.STATUS])

    @property
    def errors(self):
        """Return any errors associated with the model."""
        return self._data[self.job_type][f.ERROR_MSG]

    @property
    def runner_mode(self) -> str:
        """Returns the runner_mode of the job. May be one of ``manual`` or ``cloud``."""
        return self._data[self.job_type][f.RUNNER_MODE]

    @property
    def traceback(self) -> Optional[str]:
        """Returns the traceback associated with any job errors."""
        traceback = self._data.get(self.job_type).get(f.TRACEBACK)
        if not traceback:
            return None

        return base64.b64decode(traceback).decode("utf-8")

    @property
    def print_obj(self) -> dict:
        """Returns a printable object representation of the job"""
        out = self._data[self.job_type]
        if out.get(f.MODEL_KEY):
            del out[f.MODEL_KEY]
        return out

    @property
    def external_data_source(self) -> bool:
        """Returns ``True`` if the data source is external to Gretel Cloud.
        If the data source is a Gretel Artifact, returns ``False``.
        """
        if self.data_source:
            return not self.data_source.startswith("gretel_")
        return False

    # Base Job Methods

    def upload_data_source(self, _validate: bool = True) -> Optional[str]:
        """Resolves and uploads the data source specified in the
        model config.

        If the data source is already a Gretel artifact, the artifact
        will not be uploaded.

        Returns:
            A Gretel artifact key
        """
        if self.external_data_source and self.data_source:
            self.data_source = self.project.upload_artifact(self.data_source, _validate)
            return self.data_source

    def get_artifacts(self) -> Iterator[Tuple[str, str]]:
        """List artifact links for all known artifact types."""
        for artifact_type in self.artifact_types:
            yield artifact_type, self.get_artifact_link(artifact_type)

    def get_artifact_link(self, artifact_key: str) -> str:
        """Retrieves a signed S3 link that will download the specified
        artifact type.

        Args:
            artifact_type: Artifact type to download
        """
        if artifact_key not in self.artifact_types:
            raise Exception(
                f"artifact_key {artifact_key} not a valid key. Valid keys are {self.artifact_types}"
            )
        return self._do_get_artifact(artifact_key)

    def _peek_report(self, report_contents: dict) -> Optional[dict]:
        if self.model_type == ModelType.SYNTHETICS:
            return peek_synthetics_report(report_contents)
        if self.model_type == ModelType.TRANSFORMS:
            return peek_transforms_report(report_contents)
        if self.model_type == ModelType.CLASSIFY:
            return peek_classification_report(report_contents)

    def peek_report(self, report_path: str = None) -> Optional[dict]:
        """Return a summary of the job results

        Args:
            report_path: If a report_path is passed, that report
                will be used for the summary. If no report path
                is passed, the function will check for a cloud
                report artifact.
        """
        if not report_path:
            try:
                report_path = self.get_artifact_link("report_json")
            except Exception:
                pass
        report_contents = None
        if report_path:
            try:
                with smart_open.open(report_path, "rb") as rh:  # type:ignore
                    report_contents = rh.read()
            except Exception:
                pass
        if report_contents:
            try:
                report_contents = json.loads(report_contents)
                return self._peek_report(report_contents)
            except Exception:
                pass

    def cancel(self):
        """Cancels the active job"""
        self._poll_job_endpoint()
        if self.status in ACTIVE_STATES:
            self._do_cancel_job()

    def _poll_job_endpoint(self):
        resp = self._do_get_job_details()
        self._data = resp.get(f.DATA)

    def _check_predicate(self, start: float, wait: int = WAIT_UNTIL_DONE) -> bool:
        self._poll_job_endpoint()
        if self.status in END_STATES:
            return False
        if wait >= 0 and time.time() - start > wait:
            raise WaitTimeExceeded()
        return True

    def _new_job_logs(self) -> List[dict]:
        if self.logs and len(self.logs) > self._logs_iter_index:
            next_logs = self.logs[self._logs_iter_index :]
            self._logs_iter_index += len(next_logs)
            return next_logs
        return []

    def poll_logs_status(
        self, wait: int = WAIT_UNTIL_DONE, callback: Callable = None
    ) -> Iterator[LogStatus]:
        """Returns an iterator that may be used to tail the logs
        of a running Model

        Args:
            wait: The time in seconds to wait before closing the
                iterator. If wait is -1 (WAIT_UNTIL_DONE), the iterator will run until
                the model has reached a "completed"  or "error" state.
            callback: This function will be executed on every polling loop.
                A callback is useful for checking some external state that
                is working on a Job.
        """
        start = time.time()
        current_status = None
        while self._check_predicate(start, wait):
            if callback:
                callback()
            logs = self._new_job_logs()
            if self.status != current_status or len(logs) > 0:
                transitioned = self.status != current_status
                current_status = self.status
                yield LogStatus(status=self.status, logs=logs, transitioned=transitioned)
            time.sleep(3)

        flushed_logs = self._new_job_logs()
        if len(flushed_logs) > 0 and current_status:
            yield LogStatus(status=current_status, logs=flushed_logs, transitioned=False)

        if self.status == Status.ERROR.value:
            yield LogStatus(status=self.status, error=self.errors)
        else:
            yield LogStatus(status=self.status)

    @property
    def billing_details(self) -> dict:
        """Get billing details for the current Job"""
        return self._data.get("billing_data", {})

    @abstractproperty
    def container_image(self) -> str:
        """Return the container image for the job"""
        ...
