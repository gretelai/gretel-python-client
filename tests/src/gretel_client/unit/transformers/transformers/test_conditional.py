import pytest

from gretel_client.transformers.base import factory
from gretel_client.transformers import DataTransformPipeline, DataPath
from gretel_client.transformers import BucketRange, BucketConfig


def test_bucket(safecast_test_bucket):
    bucket_range = BucketRange(
        [(22.0, 23.0), (23.0, 24.0), (24.0, 25.0)],
        min_label="YEET",
        max_label="WOOT",
        auto_label_prefix="FEET_",
    )
    bucket_config = BucketConfig(bucket_range=bucket_range)
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


def test_string_bucket():
    bucket_range = BucketRange(
        [("a", "l"), ("m", "s")], labels=["A-L", "M-S"], max_label="T-Z"
    )
    xf = factory(BucketConfig(bucket_range=bucket_range, labels=["person_name"]))

    _, check = xf.transform_entity("person_name", "myers")
    assert check == "M-S"
    _, check = xf.transform_entity("person_name", "ehrath")
    assert check == "A-L"


def test_type_mismatch():
    bucket_range = BucketRange(
        [("a", "l"), ("m", "s")], labels=["A-L", "M-S"], max_label="T-Z"
    )
    xf = factory(BucketConfig(bucket_range=bucket_range, labels=["person_name"]))
    with pytest.raises(TypeError):
        assert not xf.transform_entity("person_name", 123)


def test_bucket2(safecast_test_bucket2):
    bucket_range = BucketRange(
        [(22.0, 23.0), (23.0, 24.0), (24.0, 25.0)],
        min_label="YEET",
        max_label="WOOT",
        auto_label_prefix="FEET_",
    )
    bucket_config = [BucketConfig(bucket_range=bucket_range)]
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

    bucket_labels = ["bearable", "perfect", "toasty", "volcano", "nuke"]
    bucket_range = BucketRange(
        [(21.0, 22.0), (22.0, 23.0), (23.0, 24.0), (24.0, 25.0), (25.0, 26.0)],
        labels=bucket_labels,
    )
    bucket_config = BucketConfig(bucket_range=bucket_range)
    data_paths = [
        DataPath(input="payload.env_temp", xforms=bucket_config),
        DataPath(input="*"),
    ]
    xf = DataTransformPipeline(data_paths)
    recs = []
    for rec in safecast_test_bucket2.get("data", {}).get("records"):
        recs.append(xf.transform_record(rec.get("data")).get("payload.env_temp"))
    assert recs == [
        "bearable",
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
