import json

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd
import smart_open

from sdmetrics.reports.single_table import QualityReport

from gretel_client.projects.models import Model

__all__ = [
    "BaseTunerMetric",
    "GretelSQSMetric",
    "SDMetricsScore",
]


class BaseTunerMetric(ABC):
    """Base class for tuner metrics."""

    def __init__(self, client=None):
        self.client = client

    @property
    def name(self):
        return self.__class__.__name__

    def _get_gretel_report(self, model: Model) -> dict:
        if (
            self.client is None
            and "azure:" in model.project.client_config.artifact_endpoint
        ):
            raise Exception(
                "A client object must be provided at initialization for Azure hybrid runs."
            )
        tp = {"client": self.client} if self.client else {}
        with smart_open.open(
            model.get_artifact_link("report_json"), mode="rb", transport_params=tp
        ) as json_file:
            report = json.load(json_file)
        return report

    @abstractmethod
    def __call__(self, model: Model) -> float:
        """Calculate the optimization metric and return the score as a float."""


class GretelSQSMetric(BaseTunerMetric):
    """Optimization metric based on the Gretel SQS."""

    def __call__(self, model: Model) -> float:
        return self._get_gretel_report(model)["synthetic_data_quality_score"][
            "raw_score"
        ]


class SDMetricsScore(BaseTunerMetric):
    """Optimization metric based on the SDMetrics SD quality score."""

    _gretel_to_sdtype = {
        "numeric": "numerical",
        "categorical": "categorical",
        "binary": "boolean",
        "other": "categorical",
    }

    def __init__(
        self,
        data_source: Union[str, Path, pd.DataFrame],
        client: Optional[Any] = None,
        metadata: Optional[Dict[str, str]] = None,
    ):
        super().__init__(client)
        self.data_source = (
            data_source
            if isinstance(data_source, pd.DataFrame)
            else pd.read_csv(data_source)
        )
        self.metadata = metadata

    def _get_metadata(self, model: Model) -> dict:
        gretel_report = self._get_gretel_report(model)
        metadata = {
            "columns": {
                f["name"]: {
                    "sdtype": self._gretel_to_sdtype[f["left_field_features"]["type"]]
                }
                for f in gretel_report["fields"]
            }
        }
        return metadata

    def _get_synthetic_data(self, model: Model) -> pd.DataFrame:
        tp = {"client": self.client} if self.client else {}
        with smart_open.open(
            model.get_artifact_link("data_preview"), mode="rb", transport_params=tp
        ) as file_in:
            df_synth = pd.read_csv(file_in)
        return df_synth

    def __call__(self, model: Model):
        self.metadata = self.metadata or self._get_metadata(model)
        sdmetrics_report = QualityReport()
        df_synth = self._get_synthetic_data(model)
        sdmetrics_report.generate(self.data_source, df_synth, self.metadata)
        return sdmetrics_report.get_score()
