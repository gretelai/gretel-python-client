"""
Bucket your data and return a single value for that bucket.
"""
from gretel_client.transformers import DataPath, DataTransformPipeline
from gretel_client.transformers.transformers import BucketConfig

min_max_width_tuple = (0.0, 3.0, 1.0)
print(BucketConfig.get_labels_from_tuple(min_max_width_tuple))
print(BucketConfig.bucket_tuple_to_list(min_max_width_tuple))

numeric_bucketing_xf = BucketConfig(
    buckets=min_max_width_tuple,
    labels=BucketConfig.get_labels_from_tuple(min_max_width_tuple),
    upper_outlier_label=3.5)

paths = [
    DataPath(input="score", xforms=numeric_bucketing_xf),
    DataPath(input="*")
]

pipe = DataTransformPipeline(paths)

records = [
    {
        "foo": "hello",
        "score": 1.234
    },
    {
        "score": 2.234
    },
    {
        "foo": "hello",
        "score": 3.234
    },
    {
        "score": -1.234
    },
]

out = [pipe.transform_record(rec) for rec in records]

# assert out == [
#     {
#         "foo": "hello",
#         "score": 1.5
#     },
#     {
#         "score": 2.5
#     },
#     {
#         "foo": "hello",
#         "score": 3.5
#     },
#     {
#         "score": 0.0
#     },
# ]

print(out)
