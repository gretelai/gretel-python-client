from abc import ABC, abstractmethod
from typing import Dict, Optional

from gretel_client.cli.utils.report_utils import generate_summary_from_legacy

StatusDescriptions = Dict[str, Dict[str, str]]


CPU = "cpu"
GPU = "gpu"


def get_status_description(
    descriptions: StatusDescriptions, status: str, runner: str
) -> str:
    status_desc = descriptions.get(status)
    if not status_desc:
        return ""
    return status_desc.get(runner, status_desc.get("default", ""))


class ModelTypeConfig(ABC):
    @property
    def train_instance_type(self) -> str:
        return CPU

    @property
    def run_instance_type(self) -> str:
        return CPU

    @property
    @abstractmethod
    def run_status_descriptions(self) -> StatusDescriptions: ...

    @property
    def train_status_descriptions(self) -> StatusDescriptions:
        return {
            "created": {
                "default": "Model creation has been queued.",
            },
            "pending": {
                "default": "A worker is being allocated to begin model creation.",
                "cloud": "A Gretel Cloud worker is being allocated to begin model creation.",
                "local": "A local container is being started to begin model creation.",
            },
            "active": {
                "default": "A worker has started creating your model!",
            },
        }

    @abstractmethod
    def peek_report(self, report_contents: dict) -> Optional[dict]: ...

    def get_report_summary(self, report_contents: dict) -> dict:
        if "summary" in report_contents:
            return {"summary": report_contents["summary"]}
        else:
            # PROD-76 Legacy report, no summary. Make one on the fly with our util helper.
            return generate_summary_from_legacy(report_contents)


class GenericModelTypeConfig(ModelTypeConfig):
    @property
    def run_status_descriptions(self) -> StatusDescriptions:
        return {
            "created": {
                "default": "A job has been queued.",
            },
            "pending": {
                "default": "A worker is being allocated to begin running.",
                "cloud": "A Gretel Cloud worker is being allocated",
                "local": "A local container is being started.",
            },
            "active": {
                "default": "A worker has started!",
            },
        }

    def peek_report(self, report_contents: dict) -> Optional[dict]:
        return None


class TransformModelTypeConfig(ModelTypeConfig):
    @property
    def run_status_descriptions(self) -> StatusDescriptions:
        return {
            "created": {
                "default": "A Record transform job has been queued.",
            },
            "pending": {
                "default": "A worker is being allocated to begin running a transform pipeline.",
                "cloud": "A Gretel Cloud worker is being allocated to begin transforming records.",
                "local": "A local container is being started to and will begin transforming records.",
            },
            "active": {
                "default": "A worker has started!",
            },
        }

    def peek_report(self, report_contents: dict) -> Optional[dict]:
        fields = [
            "training_time_seconds",
            "record_count",
            "field_count",
            "field_transforms",
            "value_transforms",
        ]
        return {f: report_contents[f] for f in fields}


class ClassifyModelTypeConfig(ModelTypeConfig):
    @property
    def run_status_descriptions(self) -> StatusDescriptions:
        return {
            "created": {
                "default": "A Record classify job has been queued.",
            },
            "pending": {
                "default": "A worker is being allocated to begin running a classification pipeline.",
                "cloud": "A Gretel Cloud worker is being allocated to begin classifying records.",
                "local": "A local container is being started and will begin classifying records.",
            },
            "active": {
                "default": "A worker has started!",
            },
        }

    def peek_report(self, report_contents: dict) -> Optional[dict]:
        fields = ["elapsed_time_seconds", "record_count", "field_count", "warnings"]
        return {f: report_contents[f] for f in fields}


class SyntheticsModelTypeConfig(ModelTypeConfig):
    @property
    def train_instance_type(self) -> str:
        return GPU

    @property
    def run_instance_type(self) -> str:
        return GPU

    @property
    def run_status_descriptions(self) -> StatusDescriptions:
        return {
            "created": {
                "default": "A Record generation job has been queued.",
            },
            "pending": {
                "default": "A worker is being allocated to begin generating synthetic records.",
                "cloud": "A Gretel Cloud worker is being allocated to begin generating synthetic records.",
                "local": "A local container is being started to begin record generation.",
            },
            "active": {
                "default": "A worker has started!",
            },
        }

    def peek_report(self, report_contents: dict) -> Optional[dict]:
        return _peek_any_report(report_contents)


class CtganModelTypeConfig(GenericModelTypeConfig):
    @property
    def train_instance_type(self) -> str:
        return GPU

    @property
    def run_instance_type(self) -> str:
        return GPU

    def peek_report(self, report_contents: dict) -> Optional[dict]:
        return _peek_any_report(report_contents)


class ActganModelTypeConfig(GenericModelTypeConfig):
    @property
    def train_instance_type(self) -> str:
        return GPU

    @property
    def run_instance_type(self) -> str:
        return GPU

    def peek_report(self, report_contents: dict) -> Optional[dict]:
        return _peek_any_report(report_contents)


class GptXModelTypeConfig(GenericModelTypeConfig):
    @property
    def train_instance_type(self) -> str:
        return GPU

    def peek_report(self, report_contents: dict) -> Optional[dict]:
        return _peek_any_report(report_contents)


class AmplifyModelTypeConfig(GenericModelTypeConfig):
    @property
    def peek_report(self, report_contents: dict) -> str:
        return _peek_any_report(report_contents)


class EvaluateModelTypeConfig(GenericModelTypeConfig):
    def peek_report(self, report_contents: dict) -> Optional[dict]:
        return _peek_any_report(report_contents)


class TimeseriesDganModelTypeConfig(GenericModelTypeConfig):
    @property
    def train_instance_type(self) -> str:
        return GPU

    @property
    def run_instance_type(self) -> str:
        return GPU


def _peek_any_report(report_contents) -> dict:
    """
    Extract the fields from any synthetic report (SQS, MQS, Text SQS).
    """
    fields = [
        "synthetic_data_quality_score",
        "field_correlation_stability",
        "principal_component_stability",
        "field_distribution_stability",
        "privacy_protection_level",
        "average_metric_difference",
        "semantic_similarity",
        "structure_similarity",
        "column_correlation_stability",
        "deep_structure_stability",
        "column_distribution_stability",
        "text_structure_similarity",
        "text_semantic_similarity",
        "membership_inference_attack_score",
        "attribute_inference_attack_score",
        "data_privacy_score",
    ]
    return {f: report_contents[f] for f in fields if f in report_contents}


_CONFIGS = {
    "synthetics": SyntheticsModelTypeConfig(),
    "transform": TransformModelTypeConfig(),
    "classify": ClassifyModelTypeConfig(),
    "ctgan": CtganModelTypeConfig(),
    "actgan": ActganModelTypeConfig(),
    "gpt_x": GptXModelTypeConfig(),
    "amplify": AmplifyModelTypeConfig(),
    "evaluate": EvaluateModelTypeConfig(),
    "timeseries_dgan": TimeseriesDganModelTypeConfig(),
    "__default__": GenericModelTypeConfig(),
}

# Specify aliases
_CONFIGS["transforms"] = _CONFIGS["transform"]


def get_model_type_config(model_type: Optional[str] = None) -> ModelTypeConfig:
    if model_type is None:
        return _CONFIGS["__default__"]

    return _CONFIGS.get(model_type, _CONFIGS["__default__"])
