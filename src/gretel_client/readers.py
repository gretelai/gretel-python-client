"""
Misc data source readers
"""

import csv
import io
import json
import os

from collections.abc import Iterator
from typing import Any, Callable, IO, List, Optional, TYPE_CHECKING, Union

import smart_open

from requests import HTTPError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from gretel_client.dataframe import _DataFrameT

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None


class ReaderError(Exception):
    pass


@retry(
    retry=retry_if_exception_type(HTTPError),
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(min=0.1, max=0.5),
)
def try_data_source(input_handle: Any) -> Any:
    """Given an input object, try to return an open file handler.
    Args:
        input_data: Input object to try and open.
    """
    if isinstance(input_handle, io.IOBase):
        return input_handle

    if isinstance(input_handle, os.PathLike):
        return open(input_handle)

    if isinstance(input_handle, str):
        return smart_open.open(input_handle)

    return input_handle


class Reader(Iterator):
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:  # pragma: no cover
        return self._name

    def __iter__(self) -> Iterator:
        return self

    def __next__(self):  # pragma: no cover
        raise NotImplementedError("__next__ not implemented.")


class JsonReader(Reader):
    def __init__(
        self,
        input_data: Union[IO[Any], str, list, dict, os.PathLike],
        mapper: Callable = lambda x: x,
    ):
        """Reads json streams or objects
        Args:
            input_data: A file-like object, string path to a file-like object,
                or a deserialized python dictionary or list.
            mapper: Function to map input objects into a transformed
                dictionary for writing. This is useful if the input stream
                isn't in the desired data structure.
        """
        input_handle = try_data_source(input_data)
        self.data_source = self._get_input_start(input_handle)
        self.mapper = mapper
        super().__init__("json")

    def _get_input_start(self, input_data):
        """Figures out the start of the json input.
        Args:
            input_data:
        """
        if isinstance(input_data, list):
            return iter(input_data)
        if isinstance(input_data, dict):
            return iter([input_data])

        record = input_data.readline().strip()

        if not record:
            return iter([])

        record_obj = json.loads(record)
        if isinstance(record_obj, list):
            return iter(record_obj)
        else:
            input_data.seek(0)
            return input_data

    def __next__(self):
        record = None
        try:
            record = next(self.data_source)
        except StopIteration:
            if callable(getattr(self.data_source, "close", None)):
                self.data_source.close()  # type: ignore
            raise StopIteration

        if isinstance(record, str):
            return self.mapper(json.loads(record.strip()))
        elif isinstance(record, object):
            return self.mapper(record)
        raise ReaderError(f"Bad object record type {type(record)}.")  # pragma: no cover


class CsvReader(Reader):
    def __init__(
        self,
        input_source: Union[IO[Any], str, os.PathLike],
        column_delimiter: str = ",",
        quote_symbol: str = '"',
        column_headings: List[str] = None,
        encoding: str = "utf-8",
        sniff: bool = True,
        schema: List = None,
        has_header: Optional[bool] = None,
    ):
        """CSV reader for reading CSV formatted files into
        Gretel's record writer.
        Args:
            input_source: a file-like object, or a string path to a file-
                like object.
            column_delimiter: One character string used to separate fields.
            quote_symbol: Symbol used to quote fields.
            column_headings: If no headers are present in the CSV this can
                be used to specify them instead.
            encoding: Set string encoding type
            sniff: If set to true the reader will use python's csv.Sniffer
                class to try and determine csv formatting.
        """
        self.column_delimiter = column_delimiter
        self.quote_symbol = quote_symbol
        self.column_headings = column_headings
        self.sniff = sniff
        self.schema = schema
        self.has_header = has_header
        self.data_source = try_data_source(input_source)

        self.try_infer_schema()

        super().__init__("csv")

    def _default_reader(self):
        return csv.reader(
            self.data_source,
            delimiter=self.column_delimiter,
            quotechar=self.quote_symbol,
        )

    def try_infer_schema(self):
        read_forward = self.data_source.read(10000)
        if not read_forward:
            return

        self.data_source.seek(0)

        if self.has_header is None:
            self.has_header = True  # assume most datasets include a header in the csv
            try:
                self.has_header = csv.Sniffer().has_header(read_forward)
            except csv.Error:
                pass

        if self.sniff:
            try:
                dialect = csv.Sniffer().sniff(read_forward)
                self.reader = csv.reader(self.data_source, dialect)
            except csv.Error:
                self.reader = self._default_reader()
        else:
            self.reader = self._default_reader()

        if not self.schema or self.has_header:
            self.schema = next(self.reader)
            self.schema = [str(h) for h in self.schema]

    def _close(self):
        if callable(getattr(self.data_source, "close", None)):
            self.data_source.close()  # type: ignore

    def __next__(self):
        if self.schema and self.reader:
            try:
                return dict(zip(self.schema, next(self.reader)))
            except StopIteration:
                self._close()
                raise
        else:
            self._close()
            raise StopIteration


class DataFrameReader(Reader):
    def __init__(self, input_data: "_DataFrameT"):
        if not pd:  # pragma: no cover
            raise RuntimeError("pandas must be installed for this reader")

        if not isinstance(input_data, pd.DataFrame):  # pragma: no cover
            raise AttributeError("input_data must be a dataframe")

        self.df = input_data
        self.source_data = self._get_input_start()

    def _get_input_start(self):
        return iter(self.df.index)

    def __next__(self):
        next_idx = next(self.source_data)

        # NOTE(jm): first using to_json() this
        # implicitly converts any Na* types to None
        # like NaN, or NaT
        record = json.loads(self.df.loc[next_idx].to_json())
        return record
