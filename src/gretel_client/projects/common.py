from enum import Enum
from pathlib import Path
from typing import Iterator, Union

from smart_open import open

from gretel_client.projects.exceptions import DataSourceError, DataValidationError
from gretel_client.readers import CsvReader, JsonReader

Pathlike = Union[str, Path]


def validate_data_source(data_source: Pathlike) -> bool:
    """Validates the input data source. Returns ``True`` if the data
    source is valid, raises an error otherwise.

    A data source is valid if we can open the file and successfully
    parse out JSON or CSV like data.

    Args:
        data_source: The data source to check.

    Raises:
        :class:`~gretel_client.projects.exceptions.DataSourceError` if the
            file can't be opened.
        :class:`~gretel_client.projects.exceptions.DataValidationError` if
            the data isn't valid CSV or JSON.
    """
    try:
        with open(data_source) as ds:
            ds.seek(1)
    except Exception as ex:
        raise DataSourceError(f"Could not open the file '{data_source}'") from ex
    try:
        peek = JsonReader(data_source)
        return _validate_from_reader(peek)
    except Exception:
        pass
    try:
        peek = CsvReader(data_source)
        return _validate_from_reader(peek)
    except Exception:
        pass
    raise DataValidationError(
        f"Data validation checks for '{data_source}' failed. "
        "Are you sure the file is valid JSON or CSV?"
    )


def _validate_from_reader(peek: Iterator, sample_size: int = 1) -> bool:
    """Perform a set of light-weight checks to ensure that the
    data is valid.

    Raises:
        :class:`~gretel_client.projects.exceptions.DataSourceError` if the
            data source can't be validated.
    """
    # TODO(dn): add additional checks to ensure the data is valid
    sample_set = None
    try:
        sample_set = [next(peek) for _ in range(sample_size)]
        assert sample_set
    except Exception as ex:
        raise DataSourceError(
            "Trying to validate data sample. "
            f"Could not read forward {sample_size} records."
        ) from ex

    return True


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
        "privacy_protection_level",
    ]
    return {f: report_contents[f] for f in fields if f in report_contents}


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
    MODEL_LOGS = "model_logs"


class ModelRunArtifact(str, Enum):
    RUN_REPORT_JSON = "run_report_json"
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
