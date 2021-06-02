import json
from typing import Any, Iterator

from gretel_client_v2.readers import CsvReader, JsonReader


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

