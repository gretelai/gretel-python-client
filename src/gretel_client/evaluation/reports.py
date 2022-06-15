from __future__ import annotations

import gzip
import json
import os

from abc import ABC, abstractproperty
from contextlib import contextmanager, nullcontext
from ctypes import Union
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterator, Optional, Union
from xmlrpc.client import boolean

import smart_open

from gretel_client.config import RunnerMode
from gretel_client.helpers import poll, submit_docker_local
from gretel_client.projects.models import Model
from gretel_client.projects.projects import Project, tmp_project

try:
    import pandas as pd
except ImportError:
    pd = None

ReportDictType = Dict[str, Any]
_model_run_exc_message = "Please run the model to generate the report."


class ModelRunException(Exception):
    ...


class BaseReport(ABC):
    """Report that can be generated for data_source and ref_data."""

    @abstractproperty
    def model_config(self) -> str:
        ...

    """Specifies a model config. For more information
    about model configs, please refer to our doc site,
    https://docs.gretel.ai/model-configurations."""

    project: Optional[Project]
    """Project associated with the report."""

    data_source: Union[Path, str, pd.DataFrame]
    """Data source used for the report."""

    ref_data: Union[Path, str, pd.DataFrame]
    """Reference data used for the report."""

    output_dir: Optional[Path]
    """Directory path to write the report to. If the directory does not exist, the path will be created for you."""

    runner_mode: RunnerMode
    """Determines where to run the model. See ``RunnerMode`` for a list of valid modes. Manual mode is not explicitly supported."""

    _report_dict: ReportDictType
    """Dictionary containing results of job run."""

    _report_html: str
    """HTML str containing results of job run."""

    _model_run: boolean = False

    def __init__(
        self,
        project: Optional[Project],
        data_source: Union[Path, str, pd.DataFrame],
        ref_data: Union[Path, str, pd.DataFrame],
        output_dir: Optional[Union[str, Path]],
        runner_mode: RunnerMode,
    ):
        self.project = project
        self.data_source = data_source
        self.ref_data = ref_data
        self.output_dir = Path(output_dir) if output_dir else os.getcwd()
        self.runner_mode = runner_mode

    def _run_model(self, model: Model):
        if self.runner_mode == RunnerMode.CLOUD:
            self._run_cloud(model=model)
        elif self.runner_mode == RunnerMode.LOCAL:
            self._run_local(model=model)

    def _run_cloud(self, model: Model):
        job = model.submit_cloud()
        poll(job)
        self._report_dict = json.loads(
            smart_open.open(job.get_artifact_link("report_json")).read()
        )
        self._report_html = smart_open.open(job.get_artifact_link("report")).read()

    def _run_local(self, model: Model):
        submit_docker_local(model, output_dir=self.output_dir)
        with gzip.open(f"{self.output_dir}/report_json.json.gz", "rt") as f:
            lines = [json.loads(line) for line in f.readlines()]
        self._report_dict = lines[0]
        with gzip.open(f"{self.output_dir}/report.html.gz", "rt") as f:
            self._report_html = f.read()

    def _run_in_project(self, project: Project):
        if pd and isinstance(self.data_source, pd.DataFrame):
            data_source_context_mgr = df_to_tmp_file
        else:
            data_source_context_mgr = nullcontext

        if pd and isinstance(self.ref_data, pd.DataFrame):
            ref_data_context_mgr = df_to_tmp_file
        else:
            ref_data_context_mgr = nullcontext

        with data_source_context_mgr(
            self.data_source
        ) as data_source, ref_data_context_mgr(self.ref_data) as ref_data:
            model = project.create_model_obj(
                self.model_config,
                data_source=str(data_source),
                ref_data=str(ref_data),
            )
            self._run_model(model=model)

    def _run(self):
        if not self.project:
            with tmp_project() as proj:
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

    def peek(self) -> ReportDictType:
        """Returns a dictionary representation of the top level report scores."""
        pass


@contextmanager
def df_to_tmp_file(df: pd.DataFrame, suffix: str = ".csv") -> Iterator[str]:
    """A temporary file context manager. Create a file that can be used inside of a "with"
    statement for temporary purposes. The file will be deleted when the scope is exited.
    """
    with NamedTemporaryFile(suffix=suffix) as df_as_file:
        df.to_csv(df_as_file.name)
        yield df_as_file.name
