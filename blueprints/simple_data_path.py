"""
Data pipeline only forwarding 2 fields, one of which get's renamed.
"""
from gretel_client.transformers import DataPath, DataTransformPipeline


paths = [
    DataPath(input="trash", output="new_trash"),
    DataPath(input="foo")
]

pipe = DataTransformPipeline(paths)

rec = {
    "foo": "hello",
    "trash": "old fish",
    "trash_again": "bad milk"
}

# Time to take out the trash
out = pipe.transform_record(rec)

assert out == {
    "foo": "hello",
    "new_trash": "old fish"
}

print(out)
