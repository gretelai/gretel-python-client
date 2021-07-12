import json
from enum import Enum
from pathlib import Path
from typing import Any, Iterator, Union

from gretel_client.readers import CsvReader, JsonReader

Pathlike = Union[str, Path]


class DataSourceError(Exception):
    """Indicates there is a problem reading the data source"""

    ...


class DataValidationError(Exception):
    """Indicates there is a problem validating the structure of the data source."""

    ...


def validate_data_source(data_source: Any):
    """Validates the input data source.

    Args:
        data_source: The data source to check.

    Raises:
        `DataSourceError` if the data source is not valid.
        `DataValidationError` if the data source doesn't pass basic structure tests.
    """
    try:
        peek = JsonReader(data_source)
        # return _validate_from_reader(peek)  TODO add this in when we support JSON files
    except (DataSourceError, json.decoder.JSONDecodeError):
        pass
    try:
        peek = CsvReader(data_source)
        return _validate_from_reader(peek)
    except DataSourceError:
        pass
    raise DataSourceError(f"Could not read or parse {data_source}")


def _validate_from_reader(peek: Iterator, sample_size: int = 1):
    """Perform a set of light-weight checks to ensure that the
    data is valid.
    """
    sample_set = None
    try:
        sample_set = [next(peek) for _ in range(sample_size)]
        assert sample_set
    except Exception as ex:
        raise DataSourceError(
            "Trying to validate data sample. "
            f"Could not read forward {sample_size} records."
        ) from ex

    # todo(dn): add additional checks to ensure the data is valid


def peek_transforms_report(report_contents: dict) -> dict:
    fields = [
        "training_time_seconds",
        "record_count",
        "field_count",
        "field_transforms",
        "value_transforms",
    ]
    return {f: report_contents[f] for f in fields}


def peek_synthetics_report(report_contents: dict) -> dict:
    fields = [
        "synthetic_data_quality_score",
        "field_correlation_stability",
        "principal_component_stability",
        "field_distribution_stability",
    ]
    return {f: report_contents[f] for f in fields}


def peek_classification_report(report_contents: dict) -> dict:
    fields = ["elapsed_time_seconds", "record_count", "field_count", "warnings"]
    return {f: report_contents[f] for f in fields}


class ModelType(str, Enum):
    SYNTHETICS = "synthetics"
    TRANSFORMS = "transforms"
    MIXED = "mixed"
    CLASSIFY = "classify"
    TMP = "__tmp__"


MANUAL = "manual"


class ModelArtifact(str, Enum):
    MODEL = "model"
    REPORT = "report"
    REPORT_JSON = "report_json"
    DATA_PREVIEW = "data_preview"
    DATA = "data"
    MODEL_LOGS = "model_logs"
    RUN_LOGS = "run_logs"


class ModelRunArtifact(str, Enum):
    REPORT = "report"
    REPORT_JSON = "report_json"
    DATA = "data"
    RUN_LOGS = "run_logs"


class f:
    """Rest api field constants."""

    DATA = "data"
    URL = "url"
    STATUS = "status"
    ERROR_MSG = "error_msg"
    TRACEBACK = "traceback"
    UID = "uid"
    MODEL_KEY = "model_key"
    WORKER_KEY = "worker_key"
    LOGS = "logs"
    MODEL = "model"
    RUNNER_MODE = "runner_mode"
    CONTAINER_IMAGE = "container_image"
    HANDLER = "handler"


YES = "yes"
NO = "no"

WAIT_UNTIL_DONE = -1
