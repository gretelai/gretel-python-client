from abc import ABC, abstractmethod, abstractproperty
import base64
from dataclasses import dataclass, field
from enum import Enum
import json
import time
from typing import Callable, Iterator, Optional, List, TYPE_CHECKING, Tuple

import smart_open

from gretel_client.config import RunnerMode
from gretel_client.rest.api.projects_api import ProjectsApi
from gretel_client.projects.common import (
    ModelType,
    f,
    peek_classification_report,
    peek_synthetics_report,
    peek_transforms_report,
    WAIT_UNTIL_DONE,
)

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


ACTIVE_STATES = [Status.CREATED, Status.ACTIVE]
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

    # Abstract methods to implement

    @abstractmethod
    def create(self) -> dict:
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
        return self._data.get(f.LOGS)

    @property
    def status(self) -> Status:
        return Status(self._data[self.job_type][f.STATUS])

    @property
    def errors(self):
        return self._data[self.job_type][f.ERROR_MSG]

    @property
    def runner_mode(self) -> RunnerMode:
        return self._data[self.job_type][f.RUNNER_MODE]

    @property
    def traceback(self) -> Optional[str]:
        traceback = self._data.get(self.job_type).get(f.TRACEBACK)
        if not traceback:
            return None

        return base64.b64decode(traceback).decode("utf-8")

    @property
    def print_obj(self) -> dict:
        """Returns an object representation of the job"""
        out = self._data[self.job_type]
        del out[f.MODEL_KEY]
        return out

    # Base Job Methods

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
        try:
            resp = self._do_get_job_details()
            self._data = resp.get(f.DATA)
        except Exception as ex:
            raise Exception(
                f"Cannot fetch {self.job_type} details for {repr(self)}"
            ) from ex

    def _check_predicate(self, start: float, wait: int = WAIT_UNTIL_DONE) -> bool:
        self._poll_job_endpoint()
        if self.status in END_STATES:
            return False
        if wait >= 0 and time.time() - start > wait:
            raise WaitTimeExceeded()
        return True

    def _new_job_logs(self) -> List[dict]:
        if self.logs and len(self.logs) > self._logs_iter_index:
            next_logs = self.logs[self._logs_iter_index:]
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

    @property
    def container_image(self) -> str:
        """Return the container image for the job"""
        return f"gretelai/{self.model_type.value}:dev"
