from gretel_client.transformers.base import factory
from gretel_client.transformers import DataTransformPipeline, DataPath
from gretel_client.transformers.transformers.bucket import (
    BucketConfig,
    bucket_creation_params_to_list,
    get_bucket_labels_from_creation_params,
    Bucket,
    BucketCreationParams,
)


def test_bucket(safecast_test_bucket2):
    bucket_list = [
        Bucket(20.0, 23.0, "Low"),
        Bucket(23.0, 24.0, "Med"),
        Bucket(24.0, 25.0, "High"),
    ]
    bucket_config = BucketConfig(buckets=bucket_list)
    data_paths = [
        DataPath(input="payload.env_temp", xforms=bucket_config),
        DataPath(input="*"),
    ]
    xf = DataTransformPipeline(data_paths)
    recs = []

    for rec in safecast_test_bucket2.get("data", {}).get("records"):
        recs.append(dict(xf.transform_record(rec.get("data"))))
    assert recs[0]["payload.env_temp"] == "Low"
    assert recs[4]["payload.env_temp"] == "Med"
    assert recs[7]["payload.env_temp"] == "High"


def test_string_bucket():
    bucket_list = [Bucket("a", "l", "a-l"), Bucket("m", "s", "m-s")]
    xf = factory(
        BucketConfig(
            buckets=bucket_list, labels=["person_name"], upper_outlier_label="t-z"
        )
    )

    _, check = xf.transform_entity("person_name", "myers")
    assert check == "m-s"
    _, check = xf.transform_entity("person_name", "ehrath")
    assert check == "a-l"


def test_type_mismatch():
    bucket_list = [Bucket("a", "l", "a-l"), Bucket("m", "s", "m-s")]
    xf = factory(
        BucketConfig(
            buckets=bucket_list, labels=["person_name"], upper_outlier_label="t-z"
        )
    )
    assert (None, 123) == xf.transform_entity("person_name", 123)


def test_bucket2(safecast_test_bucket2):
    bucket_list = [
        Bucket(22.0, 23.0, "FEET_0"),
        Bucket(23.0, 24.0, "FEET_1"),
        Bucket(24.0, 25.0, "FEET_2"),
    ]
    bucket_config = [
        BucketConfig(
            buckets=bucket_list, lower_outlier_label="YEET", upper_outlier_label="WOOT"
        )
    ]
    data_paths = [
        DataPath(input="payload.env_temp", xforms=bucket_config),
        DataPath(input="*"),
    ]
    xf = DataTransformPipeline(data_paths)

    recs = []
    for rec in safecast_test_bucket2.get("data", {}).get("records"):
        recs.append(xf.transform_record(rec.get("data")).get("payload.env_temp"))
    assert recs == [
        "YEET",
        None,
        None,
        None,
        "FEET_1",
        None,
        None,
        "WOOT",
        None,
        None,
        None,
    ]

    bucket_list = [
        Bucket(21.0, 22.0, "nice"),
        Bucket(22.0, 23.0, "bearable"),
        Bucket(23.0, 24.0, "toasty"),
        Bucket(24.0, 25.0, "volcano"),
        Bucket(25.0, 26.0, "nuke"),
    ]
    bucket_config = BucketConfig(buckets=bucket_list)
    data_paths = [
        DataPath(input="payload.env_temp", xforms=bucket_config),
        DataPath(input="*"),
    ]
    xf = DataTransformPipeline(data_paths)
    recs = []
    for rec in safecast_test_bucket2.get("data", {}).get("records"):
        recs.append(xf.transform_record(rec.get("data")).get("payload.env_temp"))
    assert recs == [
        "nice",
        None,
        None,
        None,
        "toasty",
        None,
        None,
        "nuke",
        None,
        None,
        None,
    ]


def test_config_helpers():
    buckets = bucket_creation_params_to_list(
        BucketCreationParams(0.0, 10.0, 2.5), label_method="avg"
    )
    bucket_labels = get_bucket_labels_from_creation_params(
        BucketCreationParams(0.0, 10.0, 2.5), label_method="avg"
    )
    bucket_vals = [0.0, 2.5, 5.0, 7.5, 10.0]
    bucket_label_vals = [1.25, 3.75, 6.25, 8.75]
    for idx in range(len(buckets)):
        assert abs(buckets[idx].min - bucket_vals[idx]) < 0.01
    for idx in range(len(bucket_labels)):
        assert abs(bucket_labels[idx] - bucket_label_vals[idx]) < 0.01
    assert len(buckets) == 4
    assert len(bucket_labels) == 4

    buckets = bucket_creation_params_to_list(
        BucketCreationParams(0.0, 10.0, 2.8), label_method="avg"
    )
    bucket_labels = get_bucket_labels_from_creation_params(
        BucketCreationParams(0.0, 10.0, 2.8), label_method="avg"
    )
    bucket_vals = [0.0, 2.8, 5.6, 8.4, 10.0]
    bucket_label_vals = [1.4, 4.2, 7.0, 9.8]
    for idx in range(len(buckets)):
        assert abs(buckets[idx].min - bucket_vals[idx]) < 0.01
    for idx in range(len(bucket_labels)):
        assert abs(bucket_labels[idx] - bucket_label_vals[idx]) < 0.01
    assert len(buckets) == 4
    assert len(bucket_labels) == 4


def test_type_error():
    tup = BucketCreationParams(0.0, 1.0, 0.5)
    buckets = bucket_creation_params_to_list(tup)
    paths = [DataPath(input="foo", xforms=BucketConfig(buckets=buckets))]
    pipe = DataTransformPipeline(paths)
    r = {"foo": "bar"}
    # String throws a TypeError.  We catch it and return original record.
    assert r == pipe.transform_record(r)


def test_bucketing():
    tup = BucketCreationParams(0.0, 1.0, 0.5)
    buckets = bucket_creation_params_to_list(tup, label_method="avg")
    paths = [
        DataPath(
            input="foo",
            xforms=BucketConfig(
                buckets=buckets, lower_outlier_label=0.0, upper_outlier_label=1.0
            ),
        )
    ]
    pipe = DataTransformPipeline(paths)
    r = [{"foo": "bar"}, {"foo": -1}, {"foo": 0.1}, {"foo": 0.9}, {"foo": 1.1}]
    out = [pipe.transform_record(rec) for rec in r]
    assert out == [
        {"foo": "bar"},
        {"foo": 0.0},
        {"foo": 0.25},
        {"foo": 0.75},
        {"foo": 1.0},
    ]
