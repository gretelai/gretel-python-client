"""
Bucket your numeric data and return a single value or string label for each bucket.
You can also specify a value or label for outlying values.
"""
from gretel_client.transformers import DataPath, DataTransformPipeline
from gretel_client.transformers import BucketConfig, get_bucket_labels_from_tuple

# Use a tuple for configuration.  Values are minimum, maximum and bucket width.
from gretel_client.transformers.transformers.bucket import bucket_tuple_to_list

min_max_width_tuple = (0.0, 3.0, 1.0)
buckets = bucket_tuple_to_list(min_max_width_tuple, label_method="avg")
numeric_bucketing_xf = BucketConfig(
    buckets=buckets,
    # Use a helper method to generate numeric labels.  By default, new field value is the average value of each bucket.
    # Specify output labels for field values outside the bucket range.
    lower_outlier_label=0.0,
    upper_outlier_label=3.5,
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
