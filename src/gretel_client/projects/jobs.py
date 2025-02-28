from __future__ import annotations

import base64
import json
import time

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import (
    BinaryIO,
    Callable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TYPE_CHECKING,
    Union,
)

import smart_open

import gretel_client.rest.exceptions

from gretel_client.cli.utils.parser_utils import RefData
from gretel_client.config import ClientConfig, get_logger, RunnerMode
from gretel_client.dataframe import _DataFrameT
from gretel_client.models.config import get_model_type_config
from gretel_client.projects.artifact_handlers import (
    ArtifactsHandler,
    CloudArtifactsHandler,
    get_transport_params,
    HybridArtifactsHandler,
)
from gretel_client.projects.common import f, ModelArtifact, WAIT_UNTIL_DONE
from gretel_client.projects.exceptions import (
    CreditExhaustException,
    DiscontinuedModelException,
    GretelJobNotFound,
    MaxConcurrentJobsException,
    WaitTimeExceeded,
)
from gretel_client.rest.api.projects_api import ProjectsApi
from gretel_client.rest.exceptions import ApiException

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


class Job(ABC):
    """Represents a unit of work that can be launched via
    a Gretel Worker.
    """

    project: Project
    """Project associated with the job."""

    worker_key: Optional[str]
    """Worker key used to launch the job."""

    _projects_api: ProjectsApi

    _data: Optional[dict] = None

    _not_found_error: Type[GretelJobNotFound] = GretelJobNotFound

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

    @property
    def session(self) -> ClientConfig:
        return self.project.session

    def submit(
        self,
        runner_mode: Optional[Union[str, RunnerMode]] = None,
        dry_run: bool = False,
    ) -> Job:
        """Submit this Job to the Gretel Cloud API.

        Args:
            runner_mode: Determines where to run the model. If not specified, the
                runner mode of the project (if configured) is used, otherwise
                the default runner mode of the session is used.
            dry_run: If set to True the model config will be submitted for
                validation, but won't be run. Ignored for record handlers.
        """
        if runner_mode is None:
            runner_mode = self.project.runner_mode or self.session.default_runner
        runner_mode = RunnerMode.parse(runner_mode)
        if self.project.runner_mode and runner_mode != self.project.runner_mode:
            raise ValueError(
                f"Specified runner mode '{runner_mode.value}' is different from project runner mode '{self.project.runner_mode.value}'"
            )

        if runner_mode == RunnerMode.CLOUD:
            return self.submit_cloud(dry_run)
        elif runner_mode == RunnerMode.HYBRID:
            return self.submit_hybrid(dry_run)
        elif runner_mode == RunnerMode.MANUAL:
            return self.submit_manual(dry_run)
        elif runner_mode == RunnerMode.LOCAL:
            return self.submit_local(dry_run)

    def submit_manual(self, dry_run: bool = False) -> Job:
        """Submit this Job to the Gretel Cloud API, which will create
        the job metadata but no runner will be started. The ``Model`` instance
        can now be passed into a dedicated runner.

        Returns:
            The response from the Gretel API.
        """
        return self._submit(runner_mode=RunnerMode.MANUAL, dry_run=dry_run)

    def submit_local(self, dry_run: bool = False) -> Job:
        """Submit this Job to the Gretel Cloud API to be scheduled for running in a local container.

        Returns:
            The response from the Gretel API.
        """
        return self._submit(runner_mode=RunnerMode.LOCAL, dry_run=dry_run)

    def submit_cloud(self, dry_run: bool = False) -> Job:
        """Submit this Job to the Gretel Cloud API be scheduled for running in Gretel Cloud.

        Returns:
            The response from the Gretel API.
        """
        return self._submit_remote(
            runner_mode=RunnerMode.CLOUD,
            artifacts_handler=self.project.cloud_artifacts_handler,
            dry_run=dry_run,
        )

    def submit_hybrid(self, dry_run: bool = False) -> Job:
        """Submit this Job to the Gretel Cloud API to be scheduled for running in a hybrid deployment.

        Returns:
            The response from the Gretel API.
        """
        return self._submit_remote(
            runner_mode=RunnerMode.HYBRID,
            artifacts_handler=self.project.hybrid_artifacts_handler,
            dry_run=dry_run,
        )

    def _submit_remote(
        self,
        runner_mode: RunnerMode,
        artifacts_handler: Union[CloudArtifactsHandler, HybridArtifactsHandler],
        dry_run: bool,
    ) -> Job:
        if (
            isinstance(self.data_source, _DataFrameT)
            and not self.data_source.empty
            or self.data_source
        ):
            self.upload_data_source(_artifacts_handler=artifacts_handler)

        if not self.ref_data.is_empty:
            self.upload_ref_data(_artifacts_handler=artifacts_handler)

        return self._submit(runner_mode=runner_mode, dry_run=dry_run)

    @abstractmethod
    def _submit(self, runner_mode: RunnerMode, **kwargs) -> Job: ...

    @property
    @abstractmethod
    def model_type(self) -> str: ...

    @abstractmethod
    def _do_get_job_details(self, extra_expand: Optional[list[str]] = None) -> dict: ...

    @abstractmethod
    def _do_cancel_job(self): ...

    @abstractmethod
    def delete(self): ...

    @property
    @abstractmethod
    def instance_type(self): ...

    @property
    @abstractmethod
    def artifact_types(self) -> List[str]: ...

    @abstractmethod
    def _do_get_artifact(self, artifact_key: str) -> str: ...

    # Base Job properties

    @property
    def id(self) -> Optional[str]:
        return self._job_id

    @property
    def logs(self):
        """Returns run logs for the job."""
        return self._data.get(f.LOGS)

    @property
    def status(self) -> Status:
        """The status of the job. Is one of ``gretel_client.projects.jobs.Status``."""
        return Status(self._data[self.job_type][f.STATUS])

    @property
    def errors(self):
        """Return any errors associated with the model."""
        return self._data[self.job_type][f.ERROR_MSG]

    @property
    def runner_mode(self) -> str:
        """Returns the runner_mode of the job. May be one of ``hybrid``, ``manual`` or ``cloud``."""
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
        """Returns a printable object representation of the job."""
        out = self._data[self.job_type]
        if out.get(f.MODEL_KEY):
            del out[f.MODEL_KEY]
        return out

    @property
    def external_data_source(self) -> bool:
        """Returns ``True`` if the data source is external to Gretel Cloud.
        If the data source is a Gretel Artifact, returns ``False``.
        """
        if isinstance(self.data_source, _DataFrameT):
            return True
        if self.data_source:
            return not self.data_source.startswith("gretel_")
        return False

    @property
    def external_ref_data(self) -> bool:
        """
        Returns ``True`` if the data refs are external to Gretel Cloud. If the
        data refs are Gretel Artifacts, returns ``False``.
        """
        return not self.ref_data.is_cloud_data

    # Base Job Methods

    def upload_data_source(
        self,
        _validate: bool = True,
        _artifacts_handler: Union[CloudArtifactsHandler, HybridArtifactsHandler] = None,
    ) -> Optional[str]:
        """Resolves and uploads the data source specified in the
        model config.

        If the data source is already a Gretel artifact, the artifact
        will not be uploaded.

        Returns:
            A Gretel artifact key.
        """
        if self.external_data_source and (
            (isinstance(self.data_source, _DataFrameT) and not self.data_source.empty)
            or self.data_source
        ):
            # NOTE: This assignment re-writes the gretel artifact onto the config
            self.data_source = self.project.upload_artifact(
                artifact_path=self.data_source,
                _validate=_validate,
                _artifacts_handler=_artifacts_handler,
            )
            return self.data_source

    def upload_ref_data(
        self,
        _validate: bool = True,
        _artifacts_handler: Optional[ArtifactsHandler] = None,
    ) -> RefData:
        """
        Resolves and uploads ref data sources specificed in the model config.

        If the ref data are already Gretel artifacts, we'll return
        the ref data as-is.

        Returns:
            A ``RefData`` instance that contains the new Gretel artifact values.
        """
        curr_ref_data = self.ref_data
        if curr_ref_data.is_cloud_data or curr_ref_data.is_empty:
            return curr_ref_data

        # Loop over each data source and try and upload to Gretel
        ref_data_dict = curr_ref_data.ref_dict
        for key, data_source in ref_data_dict.items():
            gretel_key = self.project.upload_artifact(
                artifact_path=data_source,
                _validate=_validate,
                _artifacts_handler=_artifacts_handler,
            )
            ref_data_dict[key] = gretel_key

        new_ref_data = RefData(ref_data_dict)

        # NOTE: This assignment re-writes the gretel artifact data onto the config
        self.ref_data = new_ref_data

        return new_ref_data

    def get_artifacts(self) -> Iterator[Tuple[str, str]]:
        """List artifact links for all known artifact types."""
        for artifact_type in self.artifact_types:
            yield artifact_type, self.get_artifact_link(artifact_type)

    def get_artifacts_by_artifact_types(
        self, artifact_types: List[str]
    ) -> Iterator[Tuple[str, str]]:
        """List artifact links for all known artifact types."""
        for artifact_type in artifact_types:
            yield artifact_type, self.get_artifact_link(artifact_type)

    def get_artifact_link(self, artifact_key: str) -> str:
        """Retrieves a signed S3 link that will download the specified
        artifact type.

        Args:
            artifact_key: Artifact type to download.
        """
        if artifact_key not in self.artifact_types:
            raise Exception(
                f"artifact_key {artifact_key} not a valid key. Valid keys are {self.artifact_types}"
            )
        return self._do_get_artifact(artifact_key)

    @contextmanager
    def get_artifact_handle(self, artifact_key: str) -> BinaryIO:
        """Returns a reference to a remote artifact that can be used to
        read binary data within a context manager

        >>> with job.get_artifact_handle("report_json") as file:
        ...   print(file.read())

        Args:
            artifact_key: Artifact type to download.

        Returns:
            a file like object
        """
        link = self.get_artifact_link(artifact_key)
        transport_params = get_transport_params(link)
        with smart_open.open(link, "rb", transport_params=transport_params) as handle:
            yield handle

    def download_artifacts(self, target_dir: Union[str, Path]):
        """Given a target directory, either as a string or a Path object, attempt to enumerate
        and download all artifacts associated with this Job

        Args:
            target_dir: The target directory to store artifacts in. If the directory does not exist,
                it will be created for you.
        """
        log = get_logger(__name__)
        output_path = Path(target_dir)
        output_path.mkdir(exist_ok=True, parents=True)
        log.info(f"Downloading model artifacts to {output_path.resolve()}")
        for artifact_type, download_link in self._get_artifacts_to_download():
            # we don't need to download cloud model artifacts
            if artifact_type == ModelArtifact.MODEL.value:
                continue
            self.project.default_artifacts_handler.download(
                download_link, output_path, artifact_type, log
            )

    def _get_artifacts_to_download(self) -> Iterator[Tuple[str, str]]:
        if self.runner_mode == "cloud":
            resp = self._do_get_job_details([f.ARTIFACTS])
            artifact_types = [
                a.get("name") for a in resp.get(f.DATA).get(f.ARTIFACTS, [])
            ]
            return self.get_artifacts_by_artifact_types(artifact_types)
        return self.get_artifacts()

    def _get_report_contents(
        self, report_path: Optional[str] = None, artifact_type: Optional[str] = None
    ) -> Optional[dict]:
        """
        Helper to encapsulate try/except boilerplate for peek_report and get_report_summary.
        """
        if report_path is None and artifact_type is not None:
            try:
                report_path = self.get_artifact_link(artifact_type)
            except Exception:
                pass

        report_contents = None
        if report_path:
            try:
                transport_params = get_transport_params(report_path)
                with smart_open.open(
                    report_path, "rb", transport_params=transport_params
                ) as rh:  # type:ignore
                    report_contents = rh.read()
            except Exception:
                pass

        if report_contents:
            try:
                return json.loads(report_contents)
            except Exception:
                pass

    def _get_report_contents_wrapper(
        self, report_path: Optional[str] = None
    ) -> Optional[dict]:
        report_contents = None
        if report_path:
            report_contents = self._get_report_contents(report_path=report_path)
        else:
            try:
                if self.model_type == "gpt_x":
                    report_contents = self._get_report_contents(
                        artifact_type="text_metrics_report_json"
                    )
                elif self.model_type == "evaluate":
                    for artifact_type in [
                        "report_json",
                        "classification_report_json",
                        "regression_report_json",
                        "text_metrics_report_json",
                    ]:
                        report_contents = self._get_report_contents(
                            artifact_type=artifact_type
                        )
                        if report_contents:
                            break
                else:
                    report_contents = self._get_report_contents(
                        artifact_type="report_json"
                    )
            except Exception:
                pass
        return report_contents

    def _peek_report(self, report_contents: dict) -> Optional[dict]:
        return get_model_type_config(self.model_type).peek_report(report_contents)

    def peek_report(self, report_path: Optional[str] = None) -> Optional[dict]:
        """Return a summary of the job results.

        Args:
            report_path: If a report_path is passed, that report
                will be used for the summary. If no report path
                is passed, the function will check for a cloud
                based artifact.
        """
        report_contents = self._get_report_contents_wrapper(report_path=report_path)

        if report_contents:
            try:
                return self._peek_report(report_contents)
            except Exception:
                pass

    def _get_report_summary(self, report_contents: dict) -> dict:
        return get_model_type_config(self.model_type).get_report_summary(
            report_contents
        )

    def get_report_summary(self, report_path: str = None) -> Optional[dict]:
        """Return a summary of the job results
        Args:
            report_path: If a report_path is passed, that report
                will be used for the summary. If no report path
                is passed, the function will check for a cloud
                report artifact.
        """
        report_contents = self._get_report_contents_wrapper(report_path=report_path)

        if report_contents:
            try:
                return self._get_report_summary(report_contents)
            except Exception:
                pass

    def cancel(self):
        """Cancels the active job."""
        self._poll_job_endpoint()
        if self.status in ACTIVE_STATES:
            self._do_cancel_job()

    def _poll_job_endpoint(self):
        try:
            resp = self._do_get_job_details()
        except gretel_client.rest.exceptions.NotFoundException as ex:
            raise self._not_found_error(self) from ex
        self._data = resp.get(f.DATA)

    def refresh(self):
        """
        Update internal state of the job by making an API call to Gretel Cloud.
        """
        self._poll_job_endpoint()

    def _check_predicate(self, start: float, wait: int = WAIT_UNTIL_DONE) -> bool:
        self._poll_job_endpoint()
        if self.status in END_STATES:
            return False
        if wait >= 0 and time.time() - start > wait:
            raise WaitTimeExceeded()
        return True

    def _new_job_logs(self) -> List[dict]:
        if self.logs and len(self.logs) > self._logs_iter_index:
            next_logs = self.logs[self._logs_iter_index :]  # noqa
            self._logs_iter_index += len(next_logs)
            return next_logs
        return []

    def poll_logs_status(
        self, wait: int = WAIT_UNTIL_DONE, callback: Callable = None
    ) -> Iterator[LogStatus]:
        """Returns an iterator that may be used to tail the logs
        of a running Model.

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
                yield LogStatus(
                    status=self.status, logs=logs, transitioned=transitioned
                )
            time.sleep(3)

        flushed_logs = self._new_job_logs()
        if len(flushed_logs) > 0 and current_status:
            yield LogStatus(
                status=current_status, logs=flushed_logs, transitioned=False
            )

        if self.status == Status.ERROR.value:
            yield LogStatus(status=self.status, error=self.errors)
        else:
            yield LogStatus(status=self.status)

    @property
    def billing_details(self) -> dict:
        """Get billing details for the current job."""
        return self._data.get("billing_data", {})

    @property
    @abstractmethod
    def container_image(self) -> str:
        """Return the container image for the job."""
        ...

    def _handle_submit_error(self, apix: ApiException) -> None:
        if "Maximum number of" in str(apix):
            raise MaxConcurrentJobsException(
                status=apix.status, reason=apix.reason
            ) from apix
        if "cannot run job, credits exhausted" in str(apix).lower():
            raise CreditExhaustException(
                status=apix.status, reason=apix.reason
            ) from apix
        if "has been discontinued" in str(apix).lower():
            # For discontinued models, include the `body` to
            # ensure we give the user a specific error message.
            exc = DiscontinuedModelException(status=apix.status, reason=apix.reason)
            exc.body = apix.body
            raise exc from apix
        raise apix
