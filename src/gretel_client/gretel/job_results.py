from abc import ABC, abstractproperty
from dataclasses import dataclass
from typing import List, Optional

try:
    import pandas as pd

    PANDAS_IS_INSTALLED = True
except ImportError:
    PANDAS_IS_INSTALLED = False

from gretel_client.dataframe import _DataFrameT
from gretel_client.gretel.artifact_fetching import (
    fetch_final_model_config,
    fetch_model_logs,
    fetch_model_report,
    fetch_synthetic_data,
    GretelReport,
)
from gretel_client.gretel.config_setup import (
    CONFIG_SETUP_DICT,
    extract_model_config_section,
)
from gretel_client.gretel.exceptions import GretelJobResultsError
from gretel_client.helpers import poll
from gretel_client.projects import Project
from gretel_client.projects.jobs import Status
from gretel_client.projects.models import Model
from gretel_client.projects.records import RecordHandler


@dataclass
class GretelJobResults(ABC):
    """Base class for Gretel jobs."""

    project: Project
    model: Model

    @property
    def model_id(self) -> str:
        return self.model.model_id

    @property
    def project_id(self) -> str:
        return self.project.project_id

    @property
    def project_url(self) -> str:
        return self.project.get_console_url()

    @abstractproperty
    def model_url(self) -> str: ...


@dataclass
class TrainJobResults(GretelJobResults):
    """Dataclass for the results from a Gretel model training job."""

    model_config: Optional[dict] = None
    report: Optional[GretelReport] = None
    model_logs: Optional[List[dict]] = None

    @property
    def model_url(self) -> str:
        return f"{self.project_url}/models/{self.model_id}/activity"

    @property
    def job_status(self) -> Status:
        self.model.refresh()
        return self.model.status

    @property
    def model_config_section(self) -> dict:
        return next(iter(self.model_config["models"][0].values()))

    def fetch_report_synthetic_data(self) -> _DataFrameT:
        """Fetch synthetic data generated for the report and return as a DataFrame.

        Note: This method requires the `pandas` package to be installed.
        """
        if not PANDAS_IS_INSTALLED:
            raise ImportError(
                "The `pandas` package is required to use this method. "
                "Please install it with `pip install pandas`."
            )
        if self.model_config_section.get("data_source") is None:
            raise GretelJobResultsError(
                "The training job did not have a data source, so "
                "no synthetic data was generated for the report."
            )
        if self.job_status != Status.COMPLETED:
            raise GretelJobResultsError(
                "The training job must be in a completed state "
                "to fetch the report synthetic data."
            )
        elif self.report is None:
            self.refresh()
        with self.model.get_artifact_handle("data_preview") as data_artifact:
            dataframe = pd.read_csv(data_artifact)
        return dataframe

    def refresh(self):
        """Refresh the training job results attributes."""
        if self.job_status == Status.COMPLETED:
            if self.model_logs is None:
                self.model_logs = fetch_model_logs(self.model)
            if self.model_config is None:
                self.model_config = fetch_final_model_config(self.model)
            if (
                self.report is None
                and self.model_config_section.get("data_source") is not None
            ):
                model_type, _ = extract_model_config_section(self.model.model_config)
                report_type = CONFIG_SETUP_DICT[model_type].report_type
                if report_type is not None:
                    self.report = fetch_model_report(self.model, report_type)

    def wait_for_completion(self):
        """Wait for the model to finish training."""
        if self.job_status != Status.COMPLETED:
            poll(self.model, verbose=False)
            self.refresh()

    def __repr__(self):
        p = ["project_id", "model_id", "job_status"]
        r = "\n".join([f"    {k}: {getattr(self, k)}" for k in p]) + "\n)\n"
        r = "(\n" + r
        return f"{self.__class__.__name__}{r}"


@dataclass
class GenerateJobResults(GretelJobResults):
    """Dataclass for the results from a Gretel data generation job."""

    record_handler: RecordHandler
    synthetic_data_link: Optional[str] = None
    synthetic_data: Optional[_DataFrameT] = None

    @property
    def model_url(self) -> str:
        return f"{self.project_url}/models/{self.model_id}/data"

    @property
    def record_id(self) -> str:
        return self.record_handler.record_id

    @property
    def job_status(self) -> Status:
        self.record_handler.refresh()
        return self.record_handler.status

    @property
    def run_logs(self) -> List[dict]:
        return self.record_handler.logs

    def refresh(self):
        """Refresh the generate job results attributes."""
        if self.job_status == Status.COMPLETED:
            if self.synthetic_data_link is None:
                self.synthetic_data_link = self.record_handler.get_artifact_link("data")
            if self.synthetic_data is None and PANDAS_IS_INSTALLED:
                self.synthetic_data = fetch_synthetic_data(self.record_handler)

    def wait_for_completion(self):
        """Wait for the model to finish generating data."""
        if self.job_status != Status.COMPLETED:
            poll(self.record_handler, verbose=False)
            self.refresh()

    def __repr__(self):
        p = ["project_id", "model_id", "record_id", "job_status"]
        r = "\n".join([f"    {k}: {getattr(self, k)}" for k in p]) + "\n)\n"
        r = "(\n" + r
        return f"{self.__class__.__name__}{r}"
