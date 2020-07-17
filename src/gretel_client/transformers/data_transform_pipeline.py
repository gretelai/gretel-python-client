import collections
from typing import List, Union

from gretel_client.transformers.data_pipeline import DataPipeline
from gretel_client.transformers.base import Transformer


GRETEL_ID = "gretel_id"
FIELDS = "fields"
METADATA = "metadata"

RECORD = "record"
DATA = "data"
RECORD_KEYS = [RECORD, DATA]


def flatten(container):
    if isinstance(container, (list, tuple)):
        for i in container:
            if isinstance(i, (list, tuple)):
                yield from flatten(i)
            else:
                yield i
    else:
        yield container


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


class DataTransformPipeline(DataPipeline):
    """This class is a container for data paths describing a records transformations.

    It constructs a data pipeline from a list of ``DataPath`` objects and is used to process records
    based on the order of the data path list. You can think of it as a bundle of data paths.

    Returns:
        An instance of ``DataTransformPipeline``
    """

    def build_datapath_list(self, data_fields) -> List[_DataPathLight]:
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

    def transform_record(self, payload: dict):
        (
            data_fields,
            record_key,
            _,
            gretel_id,
        ) = DataTransformPipeline.get_data_and_schema(payload)
        if not data_fields:
            raise ValueError("Record does not seem to contain data.")
        xform_payload_record = {}
        xform_payload_metadata_fields = {}
        data_path_list = self.build_datapath_list(data_fields)

        meta_fields = payload.get(METADATA, {}).get(FIELDS, {})
        for data_path in data_path_list:
            field = data_path.input_field
            value = data_fields.get(field)
            meta = meta_fields.get(field)
            for transformation in data_path.get_data_path_transformations():
                if transformation.labels:
                    if meta:
                        value, meta = transformation.transform_entities(value, meta)
                        if not value:
                            break
                else:
                    for key, field_ref in transformation.field_ref_dict.items():
                        if isinstance(field_ref.field_name, list):

                            transformation.field_ref_dict[key].value = [
                                xform_payload_record.get(field_name)
                                or data_fields.get(field_name)
                                for field_name in field_ref.field_name
                            ]
                        else:
                            transformation.field_ref_dict[
                                key
                            ].value = xform_payload_record.get(
                                field_ref.field_name
                            ) or data_fields.get(
                                field_ref.field_name
                            )
                    field_value = transformation.transform_field(field, value, meta)
                    if not field_value:
                        break
                    else:
                        value = field_value.get(field)
            else:
                xform_payload_record[data_path.output_field] = value
                if meta and field in meta_fields.keys():
                    xform_payload_metadata_fields[data_path.output_field] = meta
        xform_payload_record = collections.OrderedDict(
            sorted([(k, v) for k, v in xform_payload_record.items()])
        )
        return DataTransformPipeline.build_return_record(
            dict(xform_payload_record),
            record_key,
            xform_payload_metadata_fields,
            gretel_id,
        )
