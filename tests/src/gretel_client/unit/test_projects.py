import pytest
from unittest.mock import Mock, patch

import pandas as pd

from gretel_client.client import get_cloud_client, Client
from gretel_client import projects
from gretel_client.projects import Project


@pytest.fixture()
def client():
    return get_cloud_client("api", "abcd123xyz")


def test_iter_records(client: Client):
    client._iter_records = Mock()
    p = Project(name="proj", client=client, project_id=123)
    p.iter_records()
    _, _, kwargs = p.client._iter_records.mock_calls[0]
    assert kwargs["project"] == "proj"


def test_send_dataframe(client: Client):
    client._write_records = Mock()
    df = pd.DataFrame([{f"foo_{i}": "bar"} for i in range(50)])
    p = Project(name="proj", client=client, project_id=123)

    p.send_dataframe(
        df,
        sample=25,
        detection_mode="none",
        headers={"X-Test-Gretel": "one"},
        params={"test-param": "two"},
    )
    _, _, kwargs = client._write_records.mock_calls[0]
    check_df = kwargs["reader"]
    assert len(check_df.df) == 25
    assert kwargs["headers"] == {"X-Test-Gretel": "one"}
    assert kwargs["params"] == {"test-param": "two", "detection_mode": "none"}

    with pytest.raises(ValueError):
        p.send_dataframe(df, sample=0)
    with pytest.raises(ValueError):
        p.send_dataframe(df, sample=100)
    with pytest.raises(ValueError):
        p.send_dataframe(df, sample=-1)
    with pytest.raises(ValueError):
        p.send_dataframe([1, 2])

    p.send_dataframe(df, sample=0.1)
    _, _, kwargs = client._write_records.mock_calls[1]
    check_df = kwargs["reader"]
    assert len(check_df.df) == 5

    p.send_dataframe(df)
    _, _, kwargs = client._write_records.mock_calls[2]
    check_df = kwargs["reader"]
    assert len(check_df.df) == 50


@patch.object(projects, "JsonReader")
def test_send_bulk(reader, client: Client):
    client._write_records = Mock()
    records = [{"one": "test", "two": "test"}]
    p = Project(name="proj", client=client, project_id=123)
    p.send_bulk(
        records, detection_mode="all", params={"test": "param"}, headers={"test": "two"}
    )

    client._write_records.assert_called_with(
        project="proj",
        reader=reader.return_value,
        headers={"test": "two"},
        params={"test": "param", "detection_mode": "all"},
    )


def test_send(client: Client):
    client._write_record_sync = Mock(
        return_value={"data": {"success": [], "failure": []}}
    )
    records = [{"one": "test", "two": "test"}]
    p = Project(name="proj", client=client, project_id=123)
    p.send(
        records, detection_mode="all", params={"test": "param"}, headers={"test": "two"}
    )

    client._write_record_sync.assert_called_with(
        "proj",
        records,
        headers={"test": "two"},
        params={"test": "param", "detection_mode": "all"},
    )
