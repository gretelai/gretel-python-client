"""
Drop a field from the input record, or all the fields matching a blob.
"""
from gretel_client.transformers.transformers import DropConfig
from gretel_client.transformers import DataPath, DataTransformPipeline

xf = [DropConfig()]

paths = [
    DataPath(input="trash", xforms=xf),
    DataPath(input="bar*", xforms=xf),
    DataPath(input="*")
]

pipe = DataTransformPipeline(paths)

rec = {
    "foo": "hello",
    "trash": "old fish",
    "trash_again": "bad milk",
    "barry": "manilow"
}

# Time to take out the trash
out = pipe.transform_record(rec)

assert out == {
    "foo": "hello",
    "trash_again": "bad milk"
}

print(out)
