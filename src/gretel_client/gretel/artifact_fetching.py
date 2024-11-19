import json
import logging
import sys
import tempfile
import time
import webbrowser

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union

import yaml

from requests import HTTPError

try:
    import pandas as pd

    PANDAS_IS_INSTALLED = True
except ImportError:
    PANDAS_IS_INSTALLED = False

from gretel_client.dataframe import _DataFrameT
from gretel_client.projects.models import Model
from gretel_client.projects.records import RecordHandler

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


class ReportType(str, Enum):
    """The kind of report to fetch."""

    SQS = "sqs"
    TEXT = "text"
    TRANSFORM = "transform"

    @property
    def artifact_name(self) -> str:  # type: ignore
        names = {
            ReportType.SQS: "report",
            ReportType.TRANSFORM: "report",
            ReportType.TEXT: "text_metrics_report",
        }
        return names[self]


@dataclass
class GretelReport:
    """Dataclass base class for Gretel report artifacts."""

    as_dict: dict
    as_html: str

    def display_in_browser(self):
        """Display the HTML report in a browser."""
        with tempfile.NamedTemporaryFile(suffix=".html") as file:
            file.write(bytes(self.as_html, "utf-8"))
            webbrowser.open_new_tab(f"file:///{file.name}")
            time.sleep(1)

    def display_in_notebook(self):
        """Display the HTML report in a notebook."""
        from IPython.display import display, HTML

        display(HTML(data=self.as_html, metadata={"isolated": True}))

    def save_html(self, save_path: Union[str, Path]):
        """Save the HTML report to a file at the given path."""
        with open(save_path, "w") as file:
            file.write(self.as_html)

    def __repr__(self):
        return f"{self.__class__.__name__}{self.as_dict}"


@dataclass
class GretelDataQualityReport(GretelReport):
    """Dataclass for a Gretel synthetic data quality report."""

    @property
    def quality_scores(self) -> dict:
        return {d["field"]: d["value"] for d in self.as_dict["summary"]}

    def __repr__(self):
        r = "\n".join([f"    {k}: {v}" for k, v in self.quality_scores.items()])
        r = "(\n" + r + "\n)\n"
        return f"{self.__class__.__name__}{r}"


def fetch_model_logs(model: Model) -> List[dict]:
    """Fetch the logs from training a Gretel model.

    Args:
        model: The Gretel model object.

    Returns:
        A list of log messages.
    """
    model_logs = []
    try:
        with model.get_artifact_handle("model_logs") as file:
            for line in file:
                model_logs.append(json.loads(line))  # type: ignore
    except HTTPError as e:
        # If the logs artifact is not found, return an empty list
        if e.response.status_code in (404, 403):
            return model_logs
    return model_logs


def fetch_model_report(
    model: Model, report_type: ReportType = ReportType.SQS
) -> GretelReport:
    """Fetch the quality report from a model training job.

    Args:
        model: The Gretel model object.
        report_type: The type of report to fetch. One of "sqs" or "text".

    Returns:
        The Gretel report object.
    """

    report_type = ReportType(report_type)

    with model.get_artifact_handle(f"{report_type.artifact_name}_json") as file:
        report_dict = json.load(file)  # type: ignore

    with model.get_artifact_handle(report_type.artifact_name) as file:
        report_html = str(file.read(), encoding="utf-8")  # type: ignore

    if report_type in [ReportType.SQS, ReportType.TEXT]:
        return GretelDataQualityReport(as_dict=report_dict, as_html=report_html)

    return GretelReport(as_dict=report_dict, as_html=report_html)


def fetch_final_model_config(model: Model) -> dict:
    """Fetch the final model configuration from a model training job.

    Args:
        model: The Gretel model object.

    Returns:
        The final training configuration as a dict.
    """
    logs = fetch_model_logs(model)
    find_msg = "Using updated model configuration: \n"
    config_msg = [d["msg"] for d in logs if find_msg in d["msg"]]
    final_config = model.model_config
    if len(config_msg) == 1:
        try:
            i_start = len(find_msg) - 1
            final_config = yaml.safe_load(config_msg[0][i_start:])
        except Exception:
            logger.warning(
                "Unable to parse final model config from logs. "
                "Using the model object's model_config instead."
            )
    return final_config


def fetch_synthetic_data(record_handler: RecordHandler) -> _DataFrameT:
    """Fetch synthetic data from a model generate job.

    This function requires the `pandas` package to be installed.

    Args:
        record_handler: A RecordHandler object from the model.

    Raises:
        ImportError: If the `pandas` package is not installed.

    Returns:
        A pandas DataFrame containing the synthetic data.
    """
    if not PANDAS_IS_INSTALLED:
        raise ImportError(
            "The `pandas` package is required to use this function. "
            "Install it by running `pip install pandas`."
        )
    with record_handler.get_artifact_handle("data") as data_artifact:
        dataframe = pd.read_csv(data_artifact)  # type: ignore
    return dataframe
