"""This module exposes the primary class that contains all ``DataPath`` objects and is responsible for
running all transformations.
"""
import collections
from typing import TYPE_CHECKING

try:
    import pandas as pd
except ImportError:
    pd = None

from gretel_client.transformers.data_pipeline import DataPipeline
from gretel_client.readers import DataFrameReader

if TYPE_CHECKING:
    from pandas import DataFrame as DataFrameT
else:
    DataFrameT = None

GRETEL_ID = "gretel_id"
FIELDS = "fields"
METADATA = "metadata"

RECORD = "record"
DATA = "data"
RECORD_KEYS = [RECORD, DATA]


class DataTransformPipeline(DataPipeline):
    """This class is a container for data paths describing a records transformations.

    It constructs a data pipeline from a list of ``DataPath`` objects and is used to process records
    based on the order of the data path list.

    Args:
        data_paths: A list of ``data_path`` instancess
    """

    def transform_record(self, payload: dict):
        (
            data_fields,
            record_key,
            _,
            gretel_id,
        ) = DataTransformPipeline._get_data_and_schema(payload)
        if not data_fields:
            raise ValueError("Record does not seem to contain data.")
        xform_payload_record = {}
        xform_payload_metadata_fields = {}
        data_path_list = self._build_datapath_list(data_fields)

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
                    if transformation.__class__.__name__ == "Drop":
                        break
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
        return DataTransformPipeline._build_return_record(
            dict(xform_payload_record),
            record_key,
            xform_payload_metadata_fields,
            gretel_id,
        )

    def transform_df(self, df: DataFrameT) -> DataFrameT:
        """Helper method that can consume a DataFrame and iterate over each record
        as a dictionary, then run the transform pipeline on each record.
        """
        if pd is None:
            raise RuntimeError("Pandas must be installed to use this feature!")
        records = []
        reader = DataFrameReader(df)
        for row in reader:
            records.append(
                self.transform_record(row)
            )
        return pd.DataFrame(records)
