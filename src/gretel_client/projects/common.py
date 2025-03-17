from enum import Enum
from pathlib import Path
from typing import Iterable, Iterator, Union

from smart_open import open

from gretel_client.dataframe import _DataFrameT
from gretel_client.projects.exceptions import DataSourceError, DataValidationError
from gretel_client.readers import CsvReader, JsonReader

Pathlike = Union[str, Path]
DataSourceTypes = Union[str, Path, _DataFrameT]
RefDataTypes = Union[Path, str, _DataFrameT]


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

    if _has_any_extension(data_source, (".parquet", ".parq", ".tar", ".tar.gz")):
        return True

    raise DataValidationError(
        f"Data validation checks for '{data_source}' failed. "
        "Are you sure the file is valid JSON, CSV, Parquet, or (gzipped) TAR?"
    )


def _has_any_extension(data_source: Pathlike, extensions: Iterable[str]) -> bool:
    try:
        if isinstance(data_source, str):
            base_name = Path(data_source).name
        elif isinstance(data_source, Path):
            base_name = data_source.name
    except Exception:
        return False

    return any(
        len(base_name) > len(ext) and base_name.endswith(ext) for ext in extensions
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


MANUAL = "manual"


class ModelArtifact(str, Enum):
    MODEL = "model"
    REPORT = "report"
    REPORT_JSON = "report_json"
    CLASSIFICATION_REPORT = "classification_report"
    CLASSIFICATION_REPORT_JSON = "classification_report_json"
    REGRESSION_REPORT = "regression_report"
    REGRESSION_REPORT_JSON = "regression_report_json"
    TEXT_METRICS_REPORT = "text_metrics_report"
    TEXT_METRICS_REPORT_JSON = "text_metrics_report_json"
    DATA_PREVIEW = "data_preview"
    MODEL_LOGS = "model_logs"


class ModelRunArtifact(str, Enum):
    RUN_REPORT_JSON = "run_report_json"
    DATA = "data"
    RUN_LOGS = "run_logs"
    OUTPUT_FILES = "output_files"


class f:
    """Rest api field constants."""

    ARTIFACTS = "artifacts"
    KEY = "key"
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
    EMAIL = "email"


YES = "yes"
NO = "no"

WAIT_UNTIL_DONE = -1
