import json
import gzip
from pathlib import Path
from unittest.mock import patch, Mock

import pytest
import pandas as pd

from gretel_client.helpers import build_df_csv
from gretel_client.projects import Project
from gretel_client.client import Client


@pytest.fixture
def field_meta():
    return [
        {
            "field": "payload.ip_country_name",
            "count": 604974.0,
            "last_seen": "2020-05-12T14:19:23Z",
            "approx_cardinality": 5,
            "missing": 8752.0,
            "pct_missing": 1.43,
            "pct_relative_unique": 0.01,
            "pct_total_unique": 0.01,
            "s_score": 0.5,
            "types": [
                {"type": "string", "count": 604974, "last_seen": "2020-05-12T14:19:23Z"}
            ],
            "entities": [
                {
                    "label": "location",
                    "count": 604968,
                    "last_seen": "2020-05-12T14:19:23Z",
                    "approx_cardinality": 5,
                    "f_ratio": 1.0,
                }
            ],
        },
        {
            "field": "payload.ip_country_code",
            "count": 604974.0,
            "last_seen": "2020-05-12T14:19:23Z",
            "approx_cardinality": 5,
            "missing": 8752.0,
            "pct_missing": 1.43,
            "pct_relative_unique": 0.01,
            "pct_total_unique": 0.01,
            "s_score": 0.5,
            "types": [
                {"type": "string", "count": 604974, "last_seen": "2020-05-12T14:19:23Z"}
            ],
            "entities": [
                {
                    "label": "location",
                    "count": 595356,
                    "last_seen": "2020-05-12T14:19:15Z",
                    "approx_cardinality": 4,
                    "f_ratio": 0.9841,
                }
            ],
        },
        {
            "field": "payload.loc_lon",
            "count": 611844.0,
            "last_seen": "2020-05-12T14:19:23Z",
            "approx_cardinality": 5586,
            "missing": 1882.0,
            "pct_missing": 0.31,
            "pct_relative_unique": 0.92,
            "pct_total_unique": 0.92,
            "s_score": 0.51,
            "types": [
                {"type": "number", "count": 611844, "last_seen": "2020-05-12T14:19:23Z"}
            ],
            "entities": [
                {
                    "label": "longitude",
                    "count": 611836,
                    "last_seen": "2020-05-12T14:19:23Z",
                    "approx_cardinality": 5586,
                    "f_ratio": 1.0,
                },
                {
                    "label": "location",
                    "count": 545251,
                    "last_seen": "2020-05-12T14:19:23Z",
                    "approx_cardinality": 4449,
                    "f_ratio": 0.8912,
                },
            ],
        },
        {
            "field": "payload.ip_subdivision",
            "count": 487028.0,
            "last_seen": "2020-05-12T14:19:23Z",
            "approx_cardinality": 31,
            "missing": 126698.0,
            "pct_missing": 20.65,
            "pct_relative_unique": 0.01,
            "pct_total_unique": 0.01,
            "s_score": 0.4,
            "types": [
                {
                    "type": "string",
                    "count": 427423,
                    "last_seen": "2020-05-12T14:19:23Z",
                },
                {"type": "null", "count": 177551, "last_seen": "2020-05-12T14:19:15Z"},
            ],
            "entities": [
                {
                    "label": "location",
                    "count": 419625,
                    "last_seen": "2020-05-12T14:19:23Z",
                    "approx_cardinality": 20,
                    "f_ratio": 0.8616,
                },
                {
                    "label": "person_name",
                    "count": 3125,
                    "last_seen": "2020-05-12T14:12:56Z",
                    "approx_cardinality": 2,
                    "f_ratio": 0.0064,
                },
                {
                    "label": "organization_name",
                    "count": 261,
                    "last_seen": "2020-04-28T23:42:05Z",
                    "approx_cardinality": 5,
                    "f_ratio": 0.0005,
                },
            ],
        },
        {
            "field": "payload.ip_city",
            "count": 486305.0,
            "last_seen": "2020-05-12T14:19:23Z",
            "approx_cardinality": 78,
            "missing": 127421.0,
            "pct_missing": 20.77,
            "pct_relative_unique": 0.02,
            "pct_total_unique": 0.02,
            "s_score": 0.4,
            "types": [
                {
                    "type": "string",
                    "count": 426701,
                    "last_seen": "2020-05-12T14:19:23Z",
                },
                {"type": "null", "count": 178274, "last_seen": "2020-05-12T14:19:15Z"},
            ],
            "entities": [
                {
                    "label": "location",
                    "count": 348807,
                    "last_seen": "2020-05-12T14:19:23Z",
                    "approx_cardinality": 37,
                    "f_ratio": 0.7173,
                },
                {
                    "label": "person_name",
                    "count": 36132,
                    "last_seen": "2020-05-12T14:17:35Z",
                    "approx_cardinality": 11,
                    "f_ratio": 0.0743,
                },
                {
                    "label": "organization_name",
                    "count": 7247,
                    "last_seen": "2020-04-29T00:05:18Z",
                    "approx_cardinality": 12,
                    "f_ratio": 0.0149,
                },
            ],
        },
        {
            "field": "payload.loc_name",
            "count": 60471.0,
            "last_seen": "2020-05-12T14:15:23Z",
            "approx_cardinality": 27,
            "missing": 553255.0,
            "pct_missing": 90.15,
            "pct_relative_unique": 0.05,
            "pct_total_unique": 0.01,
            "s_score": 0.05,
            "types": [
                {"type": "string", "count": 60471, "last_seen": "2020-05-12T14:15:23Z"}
            ],
            "entities": [
                {
                    "label": "location",
                    "count": 92707,
                    "last_seen": "2020-05-12T14:15:23Z",
                    "approx_cardinality": 27,
                    "f_ratio": 1,
                },
                {
                    "label": "person_name",
                    "count": 87,
                    "last_seen": "2020-05-06T18:04:24Z",
                    "approx_cardinality": 1,
                    "f_ratio": 0.0014,
                },
            ],
        },
        {
            "field": "payload.location",
            "count": 611844.0,
            "last_seen": "2020-05-12T14:19:23Z",
            "approx_cardinality": 16483,
            "missing": 1882.0,
            "pct_missing": 0.31,
            "pct_relative_unique": 2.7,
            "pct_total_unique": 2.69,
            "s_score": 0.52,
            "types": [
                {"type": "string", "count": 611845, "last_seen": "2020-05-12T14:19:23Z"}
            ],
            "entities": [
                {
                    "label": "location",
                    "count": 90467,
                    "last_seen": "2020-05-12T14:18:25Z",
                    "approx_cardinality": 23,
                    "f_ratio": 0.1479,
                }
            ],
        },
        {
            "field": "payload.loc_country",
            "count": 60471.0,
            "last_seen": "2020-05-12T14:15:23Z",
            "approx_cardinality": 3,
            "missing": 553255.0,
            "pct_missing": 90.15,
            "pct_relative_unique": 0.01,
            "pct_total_unique": 0.01,
            "s_score": 0.05,
            "types": [
                {"type": "string", "count": 60471, "last_seen": "2020-05-12T14:15:23Z"}
            ],
            "entities": [
                {
                    "label": "location",
                    "count": 59761,
                    "last_seen": "2020-05-12T14:15:23Z",
                    "approx_cardinality": 3,
                    "f_ratio": 0.9883,
                }
            ],
        },
        {
            "field": "payload.loc_zone",
            "count": 60471.0,
            "last_seen": "2020-05-12T14:15:23Z",
            "approx_cardinality": 4,
            "missing": 553255.0,
            "pct_missing": 90.15,
            "pct_relative_unique": 0.01,
            "pct_total_unique": 0.01,
            "s_score": 0.05,
            "types": [
                {"type": "string", "count": 60471, "last_seen": "2020-05-12T14:15:23Z"}
            ],
            "entities": [
                {
                    "label": "location",
                    "count": 33842,
                    "last_seen": "2020-05-12T14:15:23Z",
                    "approx_cardinality": 4,
                    "f_ratio": 0.5596,
                }
            ],
        },
        {
            "field": "payload.device_sn",
            "count": 597415.0,
            "last_seen": "2020-05-12T14:19:15Z",
            "approx_cardinality": 104,
            "missing": 16311.0,
            "pct_missing": 2.66,
            "pct_relative_unique": 0.02,
            "pct_total_unique": 0.02,
            "s_score": 0.49,
            "types": [
                {"type": "string", "count": 597415, "last_seen": "2020-05-12T14:19:15Z"}
            ],
            "entities": [
                {
                    "label": "organization_name",
                    "count": 103260,
                    "last_seen": "2020-04-29T00:10:12Z",
                    "approx_cardinality": 4,
                    "f_ratio": 0.1728,
                },
                {
                    "label": "location",
                    "count": 14852,
                    "last_seen": "2020-05-12T14:17:39Z",
                    "approx_cardinality": 1,
                    "f_ratio": 0.0249,
                },
            ],
        },
        {
            "field": "payload.loc_olc",
            "count": 611844.0,
            "last_seen": "2020-05-12T14:19:23Z",
            "approx_cardinality": 8454,
            "missing": 1882.0,
            "pct_missing": 0.31,
            "pct_relative_unique": 1.39,
            "pct_total_unique": 1.38,
            "s_score": 0.51,
            "types": [
                {"type": "string", "count": 611844, "last_seen": "2020-05-12T14:19:23Z"}
            ],
            "entities": [
                {
                    "label": "location",
                    "count": 82,
                    "last_seen": "2020-05-11T13:25:37Z",
                    "approx_cardinality": 21,
                    "f_ratio": 0.0001,
                },
                {
                    "label": "person_name",
                    "count": 6,
                    "last_seen": "2020-05-05T15:03:48Z",
                    "approx_cardinality": 1,
                    "f_ratio": 0.0,
                },
            ],
        },
    ]


@pytest.fixture
def sc_records():
    p = Path(__file__).parent / "safecast_records.json.gz"
    with gzip.open(p) as fp:
        return json.loads(fp.read())


@pytest.fixture
def fields():
    return [
        "payload.ip_country_name",
        "payload.ip_country_code",
        "payload.loc_lon",
        "payload.location",
        "payload.device_sn",
        "payload.loc_olc",
    ]


@patch("gretel_client.helpers._collect_records")
def test_build_df_csv(_collect, sc_records, fields):
    _collect.return_value = sc_records
    project = Project(
        project_id=123,
        name="test",
        client=Client(host="api.gretel.cloud", api_key="123"),
    )
    check = build_df_csv(project, 5000, fields)
    assert isinstance(check, pd.DataFrame)
    assert list(check) == fields

    # now save it somewhere too
    t_path = Path(__file__).parent / "training_data.csv"
    build_df_csv(project, 5000, fields, save_to=t_path.as_posix())
    assert t_path.exists()
    t_path.unlink()
