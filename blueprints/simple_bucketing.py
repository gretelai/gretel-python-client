"""
Bucket your numeric data and return a single value or string label for each bucket.
You can also specify a value or label for outlying values.
"""
from gretel_client.transformers import (
    DataPath,
    DataTransformPipeline,
    bucket_creation_params_to_list,
    BucketCreationParams,
    BucketConfig,
    Bucket,
)


min_max_width_tuple = BucketCreationParams(0.0, 3.0, 1.0)

# NOTE: the ``label_method`` defaults to "min", but here we use "avg"
buckets = bucket_creation_params_to_list(min_max_width_tuple, label_method="avg")

numeric_bucketing_xf = BucketConfig(
    buckets=buckets, lower_outlier_label=0.0, upper_outlier_label=3.5,
)

paths = [DataPath(input="score", xforms=numeric_bucketing_xf), DataPath(input="*")]

pipe = DataTransformPipeline(paths)

records = [
    {"foo": "hello", "score": 1.234},
    {"score": 2.234},
    {"foo": "hello", "score": 3.234},
    {"score": -1.234},
]

out = [pipe.transform_record(rec) for rec in records]

assert out == [
    {"foo": "hello", "score": 1.5},
    {"score": 2.5},
    {"foo": "hello", "score": 3.5},
    {"score": 0.0},
]

print(out)


string_bucket_xf = BucketConfig(
    buckets=[Bucket("A", "L", "A-L"), Bucket("L", "S", "L-S"), Bucket("S", "Z", "S-Z")],
    lower_outlier_label="A",
    upper_outlier_label="Z",
)

paths = [DataPath(input="name", xforms=[string_bucket_xf])]

pipe = DataTransformPipeline(data_paths=paths)

recs = [
    {"name": "Artifical Intelligence"},  # A-L
    {"name": "Chevy Chase"},  # A-L
    {"name": "Lisa Simpson"},  # L-S
    {"name": "November Rain"},  # L-S
    {"name": "Samwise"},  # S-Z
    {"name": "Van Helsing"},  # S-Z
    {"name": "Zoolander"},  # Z
]

out = []

for rec in recs:
    out.append(pipe.transform_record(rec))

assert out == [
    {"name": "A-L"},
    {"name": "A-L"},
    {"name": "L-S"},
    {"name": "L-S"},
    {"name": "S-Z"},
    {"name": "S-Z"},
    {"name": "Z"},
]

print(out)
