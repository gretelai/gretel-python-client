from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

import yaml

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
    ReportType,
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
        return self.model.model_id  # type: ignore

    @property
    def project_id(self) -> str:
        return self.project.project_id

    @property
    def project_url(self) -> str:
        return self.project.get_console_url()

    @property
    @abstractmethod
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
        return next(iter(self.model_config["models"][0].values()))  # type: ignore

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
        if self.report is None and not self.model_config_section.get(
            "evaluate", {}
        ).get("skip", False):
            self.refresh()
        with self.model.get_artifact_handle("data_preview") as data_artifact:
            dataframe = pd.read_csv(data_artifact)  # type: ignore
        return dataframe

    def refresh(self):
        """Refresh the training job results attributes."""
        if self.job_status == Status.COMPLETED:
            if self.model_config is None:
                self.model_config = fetch_final_model_config(self.model)
            if (
                self.report is None
                and self.model_config_section.get("data_source") is not None
            ):
                model_type, _ = extract_model_config_section(self.model.model_config)
                report_type = CONFIG_SETUP_DICT[model_type].report_type  # type: ignore
                if report_type is not None:
                    self.report = fetch_model_report(self.model, report_type)
        # Fetch model logs no matter what
        self.model_logs = fetch_model_logs(self.model)

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
        """
        The ID of the job that generated the requested data.
        """
        return self.record_handler.record_id

    @property
    def job_status(self) -> Status:
        self.record_handler.refresh()
        return self.record_handler.status

    @property
    def run_logs(self) -> List[dict]:
        return self.record_handler.logs  # type: ignore

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


@dataclass
class TransformResults(GretelJobResults):
    """
    Should not be used directly.

    Stores metadata and a transformed DataFrame
    that was created from a Gretel Transforms job.
    """

    transform_logs: Optional[List[dict]] = None
    """Logs created during Transform job."""

    transformed_df: Optional[pd.DataFrame] = None  # type: ignore
    """A DataFrame of the transformed table. This will
    not be populated until the trasnforms job succeeds."""

    transformed_data_link: Optional[str] = None
    """URI to the transformed data (as a flat file). This will 
    not be populated until the transforms job succeeds."""

    report: Optional[GretelReport] = None

    @property
    def model_url(self) -> str:
        """
        The Gretel Console URL for the Transform model.
        """
        return f"{self.project_url}/models/{self.model_id}/data"

    @property
    def model_config(self) -> str:
        """
        The Transforms config that was used.
        """
        return yaml.safe_dump(self.model.model_config)

    @property
    def model_config_section(self) -> dict:
        return next(iter(self.model.model_config["models"][0].values()))  # type: ignore

    @property
    def job_status(self) -> Status:
        """The current status of the transform job."""
        self.model.refresh()
        return self.model.status

    def refresh(self) -> None:
        """Refresh the transform job result attributes."""
        if self.job_status == Status.COMPLETED:
            if self.transformed_data_link is None:
                self.transformed_data_link = self.model.get_artifact_link(
                    "data_preview"
                )
            if self.transformed_df is None and PANDAS_IS_INSTALLED:
                with self.model.get_artifact_handle("data_preview") as fin:
                    self.transformed_df = pd.read_csv(fin)  # type: ignore
            if (
                self.report is None
                and self.model_config_section.get("data_source") is not None
            ):
                self.report = fetch_model_report(self.model, ReportType.TRANSFORM)

        # We can fetch model logs no matter what
        self.transform_logs = fetch_model_logs(self.model)

    def wait_for_completion(self) -> None:
        """Wait for transforms job to finish running."""
        if self.job_status != Status.COMPLETED:
            poll(self.model, verbose=False)
            self.refresh()

    def __repr__(self):
        p = ["project_id", "model_id", "job_status"]
        r = "\n".join([f"    {k}: {getattr(self, k)}" for k in p]) + "\n)\n"
        r = "(\n" + r
        return f"{self.__class__.__name__}{r}"


@dataclass
class EvaluateResults(GretelJobResults):
    """
    Stores report and logs for an Evaluate job.
    """

    evaluate_logs: Optional[List[dict]] = None
    """Logs created during Evaluate job."""

    evaluate_report: Optional[GretelReport] = None
    """A report of the Evaluation job. This will not be populated until the Evaluate job succeeds."""

    @property
    def model_url(self) -> str:
        """
        The Gretel Console URL for the Evaluate model.
        """
        return f"{self.project_url}/models/{self.model_id}/data"

    @property
    def model_config(self) -> str:
        """
        The Evaluate config that was used.
        """
        return yaml.safe_dump(self.model.model_config)

    @property
    def job_status(self) -> Status:
        """The current status of the Evaluate job."""
        self.model.refresh()
        return self.model.status

    def refresh(self) -> None:
        """Refresh the Evaluate job result attributes."""
        if self.job_status == Status.COMPLETED:
            if self.evaluate_report is None:
                self.evaluate_report = fetch_model_report(self.model)

        # We can fetch model logs no matter what
        self.evaluate_logs = fetch_model_logs(self.model)

    def wait_for_completion(self) -> None:
        """Wait for Evaluate job to finish running."""
        if self.job_status != Status.COMPLETED:
            poll(self.model, verbose=False)
            self.refresh()

    def __repr__(self):
        p = ["project_id", "model_id", "job_status"]
        r = "\n".join([f"    {k}: {getattr(self, k)}" for k in p]) + "\n)\n"
        r = "(\n" + r
        return f"{self.__class__.__name__}{r}"
