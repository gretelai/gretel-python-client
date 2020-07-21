"""
Bucket your data and return a single value for that bucket.
"""
from gretel_client.transformers import DataPath, DataTransformPipeline
from gretel_client.transformers import BucketConfig, BucketRange

numeric_bucketing_xf = BucketConfig(
    bucket_range=BucketRange(
        buckets=[(0.0, 1.0), (1.0, 2.0), (2.0, 3.0)],
        labels=[0.5, 1.5, 2.5],
        min_label=0.0,
        max_label=3.5
    )
)

string_bucketing_xf = BucketConfig(
    bucket_range=BucketRange(
        buckets=[("a", "g"), ("h", "n"), ("o", "u")],
        labels=["cherry", "apple", "peach"],
        max_label="plum"
    )
)

paths = [
    DataPath(input="score", xforms=numeric_bucketing_xf),
    DataPath(input="name", xforms=string_bucketing_xf),
    DataPath(input="*")
]

pipe = DataTransformPipeline(paths)

records = [
    {
        "foo": "hello",
        "name": "adam yauch",
        "score": 1.234
    },
    {
        "name": "jerome horowitz",
        "score": 2.234
    },
    {
        "foo": "hello",
        "name": "quincy jones",
        "score": 3.234
    },
    {
        "name": "zeppo marx",
        "score": -1.234
    },
]

out = [pipe.transform_record(rec) for rec in records]

assert out == [
    {
        "foo": "hello",
        "name": "cherry",
        "score": 1.5
    },
    {
        "name": "apple",
        "score": 2.5
    },
    {
        "foo": "hello",
        "name": "peach",
        "score": 3.5
    },
    {
        "name": "plum",
        "score": 0.0
    },
]

print(out)
