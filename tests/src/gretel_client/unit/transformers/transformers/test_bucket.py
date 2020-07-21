import pytest

from gretel_client.transformers import DataTransformPipeline, DataPath
from gretel_client.transformers.transformers.bucket import BucketConfig


def test_bucket(safecast_test_bucket):
    bucket_config = BucketConfig(
        buckets=(22.0, 25.0, 1.0),
        bucket_labels=BucketConfig.get_bucket_labels_from_tuple((22.0, 25.0, 1.0)),
        lower_outlier_label="YEET",
        upper_outlier_label="WOOT"
    )
    data_paths = [
        DataPath(input="payload.loc_lat", xforms=bucket_config),
        DataPath(input="payload.loc_lon", xforms=bucket_config),
        DataPath(input="*"),
    ]
    xf = DataTransformPipeline(data_paths)
    recs = []

    for rec in safecast_test_bucket.get("data", {}).get("records"):
        recs.append(dict(xf.transform_record(rec.get("data"))))
    assert recs == [
        {
            "origin": "arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd",
            "payload.bat_voltage": 7.64,
            "payload.dev_comms_failures": 534,
            "payload.dev_free_memory": 53636,
            "payload.dev_last_failure": "FAILsdcard",
            "payload.dev_ntp_count": 1,
            "payload.dev_restarts": 648,
            "payload.device": 10002,
            "payload.device_class": "pointcast",
            "payload.device_sn": "Pointcast #10002",
            "payload.device_urn": "pointcast:10002",
            "payload.env_temp": 21.1,
            "payload.ip_address": "122.212.234.10",
            "payload.ip_city": "Shibuya",
            "payload.ip_country_code": "JP",
            "payload.ip_country_name": "Japan",
            "payload.ip_subdivision": "Tokyo",
            "payload.loc_alt": 92,
            "payload.loc_lat": "WOOT",
            "payload.loc_lon": "WOOT",
            "payload.loc_olc": "8Q7XMP5H+Q4X",
            "payload.location": "35.659491,139.72785",
            "payload.service_handler": "i-051cab8ec0fe30bcd",
            "payload.service_md5": "abf7a122a5a0c20588d239199c8c6d7f",
            "payload.service_transport": "pointcast:122.212.234.10",
            "payload.service_uploaded": "2020-03-10T23:58:55Z",
            "payload.when_captured": "2020-03-10T23:58:55Z",
        },
        {
            "origin": "arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd",
            "payload.device": 10002,
            "payload.device_class": "pointcast",
            "payload.device_sn": "Pointcast #10002",
            "payload.device_urn": "pointcast:10002",
            "payload.ip_address": "122.212.234.10",
            "payload.ip_city": "Shibuya",
            "payload.ip_country_code": "JP",
            "payload.ip_country_name": "Japan",
            "payload.ip_subdivision": "Tokyo",
            "payload.lnd_7128ec": 25,
            "payload.loc_alt": 92,
            "payload.loc_lat": "WOOT",
            "payload.loc_lon": "WOOT",
            "payload.loc_olc": "8Q7XMP5H+Q4X",
            "payload.location": "35.659491,139.72785",
            "payload.service_handler": "i-051a2a353509414f0",
            "payload.service_md5": "cb93606463ba99994f832177e39dc6a5",
            "payload.service_transport": "pointcast:122.212.234.10",
            "payload.service_uploaded": "2020-03-10T23:58:54Z",
            "payload.when_captured": "2020-03-10T23:58:54Z",
        },
        {
            "origin": "arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd",
            "payload.device": 10002,
            "payload.device_class": "pointcast",
            "payload.device_sn": "Pointcast #10002",
            "payload.device_urn": "pointcast:10002",
            "payload.ip_address": "122.212.234.10",
            "payload.ip_city": "Shibuya",
            "payload.ip_country_code": "JP",
            "payload.ip_country_name": "Japan",
            "payload.ip_subdivision": "Tokyo",
            "payload.lnd_7318u": 12,
            "payload.loc_alt": 92,
            "payload.loc_lat": "WOOT",
            "payload.loc_lon": "WOOT",
            "payload.loc_olc": "8Q7XMP5H+Q4X",
            "payload.location": "35.659491,139.72785",
            "payload.service_handler": "i-0c65ac97805549e0d",
            "payload.service_md5": "72f2b7cf2132bcc50ea68a2b6bdb6e2d",
            "payload.service_transport": "pointcast:122.212.234.10",
            "payload.service_uploaded": "2020-03-10T23:58:54Z",
            "payload.when_captured": "2020-03-10T23:58:54Z",
        },
    ]


def test_value_errors():
    with pytest.raises(ValueError):
        DataPath(input="foo", xforms=BucketConfig(buckets=[]))
    with pytest.raises(ValueError):
        DataPath(input="foo", xforms=BucketConfig())
    with pytest.raises(ValueError):
        BucketConfig.bucket_tuple_to_list()
    with pytest.raises(ValueError):
        BucketConfig.get_bucket_labels_from_tuple((0.0, 1.0, 0.5), method="foo")
    with pytest.raises(ValueError):
        DataPath(input="foo", xforms=BucketConfig(buckets=[0.0, 1.0, 2.0], bucket_labels=[0.0, 1.0, 2.0]))


def test_config_helpers():
    buckets = BucketConfig.bucket_tuple_to_list((0.0, 10.0, 2.5))
    bucket_labels = BucketConfig.get_bucket_labels_from_tuple((0.0, 10.0, 2.5))
    bucket_vals = [0.0, 2.5, 5.0, 7.5, 10.0]
    bucket_label_vals = [1.25, 3.75, 6.25, 8.75]
    for idx in range(len(buckets)):
        assert abs(buckets[idx] - bucket_vals[idx]) < 0.01
    for idx in range(len(bucket_labels)):
        assert abs(bucket_labels[idx] - bucket_label_vals[idx]) < 0.01
    assert len(buckets) == 5
    assert len(bucket_labels) == 4

    buckets = BucketConfig.bucket_tuple_to_list((0.0, 10.0, 2.8))
    bucket_labels = BucketConfig.get_bucket_labels_from_tuple((0.0, 10.0, 2.8))
    bucket_vals = [0.0, 2.8, 5.6, 8.4, 10.0]
    bucket_label_vals = [1.4, 4.2, 7.0, 9.8]
    for idx in range(len(buckets)):
        assert abs(buckets[idx] - bucket_vals[idx]) < 0.01
    for idx in range(len(bucket_labels)):
        assert abs(bucket_labels[idx] - bucket_label_vals[idx]) < 0.01
    assert len(buckets) == 5
    assert len(bucket_labels) == 4


def test_type_error():
    tup = (0.0, 1.0, 0.5)
    paths = [DataPath(
        input="foo",
        xforms=BucketConfig(buckets=tup, bucket_labels=BucketConfig.get_bucket_labels_from_tuple(tup)))]
    pipe = DataTransformPipeline(paths)
    r = {"foo": "bar"}
    # String throws a TypeError.  We catch it and return original record.
    assert r == pipe.transform_record(r)


def test_bucketing():
    tup = (0.0, 1.0, 0.5)
    paths = [DataPath(
        input="foo",
        xforms=BucketConfig(
            buckets=tup,
            bucket_labels=BucketConfig.get_bucket_labels_from_tuple(tup),
            lower_outlier_label=0.0,
            upper_outlier_label=1.0
        ))]
    pipe = DataTransformPipeline(paths)
    r = [
        {
            "foo": "bar"
        },
        {
            "foo": -1
        },
        {
            "foo": 0.1
        },
        {
            "foo": 0.9
        },
        {
            "foo": 1.1
        }
    ]
    out = [pipe.transform_record(rec) for rec in r]
    assert out == [
        {
            "foo": "bar"
        },
        {
            "foo": 0.0
        },
        {
            "foo": 0.25
        },
        {
            "foo": 0.75
        },
        {
            "foo": 1.0
        }
    ]
