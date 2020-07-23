"""
Drop a field from the input record, or all the fields matching a blob.
"""
from gretel_client.transformers import CombineConfig
from gretel_client.transformers import DataPath, DataTransformPipeline, FieldRef

xf_combine = CombineConfig(
    combine=FieldRef(["first_name", "city", "state"]), separator=", "
)

data_paths = [
    DataPath(input="last_name", xforms=xf_combine, output="name_location"),
]

pipe = DataTransformPipeline(data_paths)

rec = {"first_name": "James", "last_name": "Bond", "city": "London", "state": "WI"}

# Time to take out the trash
out = pipe.transform_record(rec)

assert out == {"name_location": "Bond, James, London, WI"}

print(out)
