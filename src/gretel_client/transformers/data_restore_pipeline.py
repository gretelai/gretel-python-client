import collections
from typing import List

from gretel_client.transformers.data_pipeline import DataPipeline
from gretel_client.transformers.data_transform_pipeline import (
    DataTransformPipeline,
    _DataPathLight,
    METADATA,
    FIELDS,
)
from gretel_client.transformers.restore import RestoreTransformer


class DataRestorePipeline(DataPipeline):
    """This class is a container for data paths describing a records transformations.

    It constructs a data pipeline from a list of ``DataPath`` objects and is used to process records
    based on the order of the data path list. You can think of it as a bundle of data paths.

    Returns:
        An instance of ``DataTransformPipeline``
    """

    def build_datapath_list_restore(self, data_fields) -> List[_DataPathLight]:
        data_path_list = []
        record_fields = set(data_fields.keys())
        fields_to_process = []
        for data_path in self.data_paths:
            record_fields_processed = set()
            for data_field in record_fields:
                if data_path.input_field_matcher.match(data_field):
                    fields_to_process.append(data_field)
                    record_fields_processed.add(data_field)
            record_fields -= record_fields_processed

        for input_field in reversed(fields_to_process):
            data_path = None
            output_field = None
            for _data_path in self.data_paths:
                if input_field == _data_path.output_field:
                    data_path = _data_path
                    output_field = data_path.input_field
                    break
            else:
                for _data_path in self.data_paths:
                    if _data_path.input_field_matcher.match(input_field):
                        data_path = _data_path
                        output_field = input_field
                        break

            if data_path:
                data_path_list.append(
                    _DataPathLight(input_field, data_path.transformations, output_field)
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
        data_path_list = self.build_datapath_list_restore(data_fields)

        meta_fields = payload.get(METADATA, {}).get(FIELDS, {})
        for data_path in data_path_list:
            field = data_path.input_field
            value = data_fields.get(field)
            meta = meta_fields.get(field)
            for transformation in reversed(data_path.get_data_path_transformations()):
                if transformation.labels:
                    if meta:
                        if isinstance(transformation, RestoreTransformer):
                            value, meta = transformation.restore_entities(value, meta)
                        else:
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
                    if isinstance(transformation, RestoreTransformer):
                        field_value = transformation.restore_field(field, value, meta)
                    else:
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
            gretel_id
        )
