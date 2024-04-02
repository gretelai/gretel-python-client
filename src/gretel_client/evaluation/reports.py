from __future__ import annotations

import gzip
import json
import os
import time

from abc import ABC, abstractproperty
from ctypes import Union
from pathlib import Path
from typing import Any, Dict, Optional, Union
from xmlrpc.client import boolean

import smart_open

from gretel_client.config import (
    ClientConfig,
    get_logger,
    get_session_config,
    RunnerMode,
)
from gretel_client.helpers import submit_docker_local
from gretel_client.projects.common import DataSourceTypes, RefDataTypes
from gretel_client.projects.jobs import END_STATES, Job, Status
from gretel_client.projects.models import Model
from gretel_client.projects.projects import Project, tmp_project

ReportDictType = Dict[str, Any]
_model_run_exc_message = "Please run the model to generate the report."

DEFAULT_CORRELATION_COLUMNS = 75
DEFAULT_SQS_REPORT_COLUMNS = 250
DEFAULT_RECORD_COUNT = 5000
DEFAULT_TEXT_RECORD_COUNT = 80


class ModelRunException(Exception): ...


class BaseReport(ABC):
    """Report that can be generated for data_source and ref_data."""

    @abstractproperty
    def model_config(self) -> dict: ...

    @abstractproperty
    def base_artifact_name(self) -> str: ...

    """Specifies a model config. For more information
    about model configs, please refer to our doc site,
    https://docs.gretel.ai/reference/model-configurations."""

    project: Optional[Project]
    """Optional project associated with the report. If no project is passed, a temp project (``tmp_project``) will be used."""

    data_source: DataSourceTypes
    """Data source used for the report."""

    ref_data: RefDataTypes
    """Reference data used for the report."""

    test_data: RefDataTypes
    """Additional optional test data used for MQS reports."""

    output_dir: Optional[Path]
    """Optional directory path to write the report to. If the directory does not exist, the path will be created for you."""

    runner_mode: RunnerMode
    """Determines where to run the model. See ``RunnerMode`` for a list of valid modes. Manual mode is not explicitly supported."""

    session: ClientConfig

    _report_dict: ReportDictType
    """Dictionary containing results of job run."""

    _report_html: str
    """HTML str containing results of job run."""

    _model_run: boolean = False

    def __init__(
        self,
        project: Optional[Project],
        data_source: DataSourceTypes,
        ref_data: RefDataTypes,
        output_dir: Optional[Union[str, Path]],
        runner_mode: RunnerMode,
        test_data: Optional[RefDataTypes] = None,
        *,
        session: Optional[ClientConfig] = None,
    ):
        project, session = BaseReport.resolve_session(project, session)
        self.session = session
        self.project = project
        self.data_source = (
            str(data_source) if isinstance(data_source, Path) else data_source
        )
        self.ref_data = str(ref_data) if isinstance(ref_data, Path) else ref_data
        self.test_data = str(test_data) if isinstance(test_data, Path) else test_data
        self.output_dir = Path(output_dir) if output_dir else os.getcwd()
        self.runner_mode = runner_mode

    @staticmethod
    def resolve_session(
        project: Optional[Project], session: Optional[ClientConfig]
    ) -> tuple[Optional[Project], ClientConfig]:
        if session is None:
            if project is None:
                session = get_session_config()
            else:
                session = project.session
        elif project is not None:
            # If a session is explicitly specified, the project's session
            # should match it.
            project = project.with_session(session)
        return project, session

    def _run_model(self, model: Model):
        if self.runner_mode == RunnerMode.CLOUD:
            self._run_cloud(model=model)
        elif self.runner_mode == RunnerMode.LOCAL:
            self._run_local(model=model)
        elif self.runner_mode == RunnerMode.HYBRID:
            self._run_hybrid(model=model)

    def _await_completion(self, job: Job):
        refresh_attempts = 0
        log = get_logger(__name__)
        while True:
            exception = None
            if refresh_attempts >= 5:
                raise ModelRunException("Lost contact with job") from exception

            time.sleep(10)

            try:
                job.refresh()
                refresh_attempts = 0
            except Exception as e:
                exception = e
                refresh_attempts = refresh_attempts + 1
                attempts_remaining = 5 - refresh_attempts
                log.debug(
                    f"Failed to refresh job status. Will re-attempt up to {attempts_remaining} more times. {e}"
                )

            status = job.status

            if status == Status.COMPLETED:
                break
            elif status in END_STATES:
                raise ModelRunException("Job finished unsuccessfully")
            else:
                continue

    def _run_cloud(self, model: Model):
        job = model.submit_cloud()
        self._run_remote(job)

    def _run_hybrid(self, model: Model):
        job = model.submit_hybrid()
        self._run_remote(job)

    def _run_remote(self, job: Job):
        self._await_completion(job)

        with smart_open.open(
            job.get_artifact_link(f"{self.base_artifact_name}_json")
        ) as f:
            self._report_dict = json.load(f)
        with smart_open.open(
            job.get_artifact_link(f"{self.base_artifact_name}"), encoding="utf8"
        ) as f:
            self._report_html = f.read()

    def _run_local(self, model: Model):
        submit_docker_local(model, output_dir=self.output_dir)
        with gzip.open(
            f"{self.output_dir}/{self.base_artifact_name}_json.json.gz", "rt"
        ) as f:
            lines = [json.loads(line) for line in f.readlines()]
        self._report_dict = lines[0]
        with gzip.open(
            f"{self.output_dir}/{self.base_artifact_name}.html.gz", "rt"
        ) as f:
            self._report_html = f.read()

    def _run_in_project(self, project: Project):
        if self.test_data is not None:
            _ref_data = [self.ref_data, self.test_data]
        else:
            _ref_data = self.ref_data
        model = project.create_model_obj(
            self.model_config,
            data_source=self.data_source,
            ref_data=_ref_data,
        )
        self._run_model(model=model)

    def _run(self):
        if not self.project:
            with tmp_project(session=self.session) as proj:
                self._run_in_project(proj)
        else:
            self._run_in_project(self.project)
        self._model_run = True

    def run(self):
        self._run()

    def _check_model_run(self):
        if not self._model_run:
            raise ModelRunException(_model_run_exc_message)

    @property
    def as_dict(self) -> ReportDictType:
        """Returns a dictionary representation of the report."""
        self._check_model_run()
        return self._report_dict

    @property
    def as_html(self) -> str:
        """Returns a HTML representation of the report."""
        self._check_model_run()
        return self._report_html

    def peek(self) -> Optional[ReportDictType]:
        """Returns a dictionary representation of the top level report scores."""
        pass
