import json

from abc import ABC, abstractmethod
from enum import Enum
from typing import List

from gretel_client.gretel.config_setup import (
    CONFIG_SETUP_DICT,
    extract_model_config_section,
    ModelName,
)
from gretel_client.projects.models import Model


class GretelMetricName(str, Enum):
    SQS = "synthetic_data_quality_score"
    FCS = "field_correlation_stability"
    PCS = "principal_component_stability"
    FDS = "field_distribution_stability"
    TEXT_STRUCTURE = "text_structure_similarity"
    TEXT_SEMANTIC = "text_semantic_similarity"

    @property
    def compatible_models(self) -> List[ModelName]:
        if self == GretelMetricName.SQS:
            return [
                ModelName.ACTGAN,
                ModelName.AMPLIFY,
                ModelName.LSTM,
                ModelName.TABULAR_DP,
                ModelName.GPT_X,
            ]
        elif self in [
            GretelMetricName.FCS,
            GretelMetricName.PCS,
            GretelMetricName.FDS,
        ]:
            return [
                ModelName.ACTGAN,
                ModelName.AMPLIFY,
                ModelName.LSTM,
                ModelName.TABULAR_DP,
            ]
        elif self in [
            GretelMetricName.TEXT_STRUCTURE,
            GretelMetricName.TEXT_SEMANTIC,
        ]:
            return [ModelName.GPT_X]


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
