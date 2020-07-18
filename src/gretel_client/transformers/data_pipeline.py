"""
This module contains the base class for both "forward" and "reverse" transformer pipelines.

The primary interface that this module makes available to users is the ``DataPath`` class.
"""
import fnmatch
import re
from abc import ABC, abstractmethod
from typing import List, Union, Tuple

try:
    from re import Pattern
except ImportError:
    from typing import Pattern

from gretel_client.transformers.base import Transformer, TransformerConfig, factory

GRETEL_ID = "gretel_id"
FIELDS = "fields"
METADATA = "metadata"

RECORD = "record"
DATA = "data"
RECORD_KEYS = [RECORD, DATA]


def _flatten(container):
    if isinstance(container, (list, tuple)):
        for i in container:
            if isinstance(i, (list, tuple)):
                yield from _flatten(i)
            else:
                yield i
    else:
        yield container


class DataPath:
    """This class is a container for transformers describing each records transformations. It can also be used to
    transform the output field name.

    It constructs the transformer instances from a list of transformer config objects and is used to process records
    based on the order of transformations as specified by the order of the transformer config object list.

    Args:
        input: input field name. Can be glob patterns similar to file path matching.
        xforms: list of transformer config objects. These transformations will be applied to the data.
        output: output field name. The output value will be written into this key.

    Returns:
        An instance of ``DataPath``
    """

    input_field_matcher: Pattern
    input_field: str
    output_field: str
    transformations: List[Transformer] = None

    def __init__(
        self,
        *,
        input: str,
        xforms: Union[List[TransformerConfig], TransformerConfig] = None,
        output: str = None
    ):
        self.input_field_matcher = re.compile(fnmatch.translate(input))
        self.input_field = input
        self.output_field = output
        if xforms:
            transform_configs = list(_flatten(xforms))
            self.transformations = [factory(config) for config in transform_configs]


class _DataPathLight:
    input_field: str
    output_field: str
    transformations: List[Transformer] = None

    def __init__(
        self,
        input_field: str,
        transformations: Union[List[Transformer], Transformer] = None,
        output_field: str = None,
    ):
        self.input_field = input_field
        self.output_field = output_field
        self.transformations = transformations

    def get_data_path_transformations(self):
        return self.transformations or []


class DataPipeline(ABC):
    """This class is a container for data paths describing a records transformations.

    It constructs a data pipeline from a list of ``DataPath`` objects and is used to process records
    based on the order of the data path list. You can think of it as a bundle of data paths.

    Args:
        data_paths: a list of data paths containing the desired input fields for processing.
    """

    data_paths: List[DataPath]

    def __init__(self, data_paths: List[DataPath]):
        self.data_paths = data_paths

    @staticmethod
    def _get_data_and_schema(input_payload) -> Tuple[dict, str, dict, str]:
        """Given the payload to transform, check if the payload came from the Gretel
        API or is just a vanilla dict payload to transform.

        We use the existence of the following Gretel record structure to determine if
        it came from the Gretel API::

            {
                "data": {},
                "metadata": {
                    "gretel_id": "..."
                }
            }

        If the ``input_payload`` does not have this structure, we assume it
        is just a regular dictionary to be transformed.
        """
        record_key = [key for key in RECORD_KEYS if key in input_payload]
        meta_data = input_payload.get(METADATA, {})
        gretel_id = meta_data.get(GRETEL_ID)
        if meta_data and gretel_id:
            record_data = input_payload.get(DATA) or input_payload.get(RECORD)
            if record_key:
                record_key = record_key[0]
        else:
            record_data = input_payload
        return record_data, record_key, meta_data, gretel_id

    @staticmethod
    def _build_return_record(record_data, record_key, meta_data_fields, gretel_id):
        if record_key in RECORD_KEYS:
            return {
                record_key: record_data,
                METADATA: {GRETEL_ID: gretel_id, FIELDS: meta_data_fields},
            }
        else:
            return record_data

    def _build_datapath_list(self, data_fields) -> List[_DataPathLight]:
        data_path_list = []
        fields_to_process = set(data_fields.keys())
        for data_path in self.data_paths:
            fields = [
                data_field
                for data_field in fields_to_process
                if data_path.input_field_matcher.match(data_field)
            ]
            for field in fields:
                fields_to_process.remove(field)
                output = data_path.output_field or field
                data_path_list.append(
                    _DataPathLight(field, data_path.transformations, output)
                )
        return data_path_list

    @abstractmethod
    def transform_record(self, payload: dict) -> dict:
        pass
