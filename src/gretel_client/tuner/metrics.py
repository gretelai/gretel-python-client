import json

from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

from gretel_client.gretel.artifact_fetching import fetch_synthetic_data
from gretel_client.gretel.config_setup import (
    CONFIG_SETUP_DICT,
    extract_model_config_section,
    ModelType,
)
from gretel_client.gretel.exceptions import GretelJobSubmissionError
from gretel_client.helpers import poll
from gretel_client.projects.models import Model


class GretelMetricName(str, Enum):
    SQS = "synthetic_data_quality_score"
    FCS = "field_correlation_stability"
    PCS = "principal_component_stability"
    FDS = "field_distribution_stability"
    TEXT_STRUCTURE = "text_structure_similarity"
    TEXT_SEMANTIC = "text_semantic_similarity"

    @property
    def compatible_models(self) -> List[ModelType]:
        if self == GretelMetricName.SQS:
            return [
                ModelType.ACTGAN,
                ModelType.AMPLIFY,
                ModelType.LSTM,
                ModelType.TABULAR_DP,
                ModelType.GPT_X,
            ]
        elif self in [
            GretelMetricName.FCS,
            GretelMetricName.PCS,
            GretelMetricName.FDS,
        ]:
            return [
                ModelType.ACTGAN,
                ModelType.AMPLIFY,
                ModelType.LSTM,
                ModelType.TABULAR_DP,
            ]
        elif self in [
            GretelMetricName.TEXT_STRUCTURE,
            GretelMetricName.TEXT_SEMANTIC,
        ]:
            return [ModelType.GPT_X]


class MetricDirection(str, Enum):
    MAXIMIZE = "maximize"
    MINIMIZE = "minimize"


class BaseTunerMetric(ABC):
    """Base class for Tuner optimization metrics.

    Notes::
        To work with `GretelTuner`, metrics must subclass this base class and implement
        a __call__ method that takes a Gretel `Model` as input and returns the metric
        score as a float. By default, the metric is maximized. To minimize the metric,
        set the `direction` attribute to `MetricDirection.MINIMIZE`.
    """

    direction: MetricDirection = MetricDirection.MAXIMIZE

    def get_gretel_report(self, model: Model) -> dict:
        """Get the Gretel synthetic data quality report."""
        model_type, _ = extract_model_config_section(model.model_config)
        report_type = CONFIG_SETUP_DICT[model_type].report_type
        if report_type is None:
            raise ValueError(
                f"Model type {model_type.upper()} does not generate a report and "
                "therefore does not have a quality score."
            )
        with model.get_artifact_handle(f"{report_type.artifact_name}_json") as file:
            report = json.load(file)
        return report

    def submit_generate_for_trial(
        self,
        model: Model,
        num_records: Optional[int] = None,
        seed_data: Optional[Union[str, Path, pd.DataFrame]] = None,
        **generate_kwargs,
    ) -> pd.DataFrame:
        """Submit generate job for hyperparameter tuning trial.

        Only one of `num_records` or `seed_data` can be provided. The former
        will generate a complete synthetic dataset, while the latter will
        conditionally generate synthetic data based on the seed data.

        Args:
            model: Gretel `Model` instance.
            num_records: Number of records to generate.
            seed_data: Seed data source as a file path or pandas DataFrame.

        Raises:
            TypeError: If `model` is not a Gretel `Model` instance.
            GretelJobSubmissionError: If the combination of arguments is invalid.

        Returns:
            Pandas DataFrame containing the synthetic data.
        """
        if not isinstance(model, Model):
            raise TypeError(f"Expected a Gretel Model object, got {type(model)}.")

        if num_records is not None and seed_data is not None:
            raise GretelJobSubmissionError(
                "Only one of `num_records` or `seed_data` can be provided."
            )

        if num_records is None and seed_data is None:
            raise GretelJobSubmissionError(
                "Either `num_records` or `seed_data` must be provided."
            )

        if num_records is not None:
            generate_kwargs.update({"num_records": num_records})

        data_source = str(seed_data) if isinstance(seed_data, Path) else seed_data

        record_handler = model.create_record_handler_obj(
            data_source=data_source,
            params=generate_kwargs,
        )

        record_handler.submit()

        poll(record_handler, verbose=False)

        return fetch_synthetic_data(record_handler)

    @abstractmethod
    def __call__(self, model: Model) -> float:
        """Calculate the optimization metric and return the score as a float."""

    def __repr__(self):
        return f"{self.__class__.__name__}(direction: {self.direction.value})"


class GretelQualityScore(BaseTunerMetric):
    """Tuner Optimization metric based on Gretel's Synthetic Data Quality Report."""

    def __init__(self, metric_name: GretelMetricName = GretelMetricName.SQS):
        self.metric_name = GretelMetricName(metric_name)
        self.direction = (
            MetricDirection.MAXIMIZE
            if self.metric_name == GretelMetricName.SQS
            else MetricDirection.MINIMIZE
        )
        self._report_key = self.metric_name.value.replace("text_", "")

    def __call__(self, model: Model) -> float:
        return self.get_gretel_report(model)[self._report_key]["raw_score"]

    def __repr__(self):
        return f"{self.metric_name.value.upper()}(direction: {self.direction.value})"
