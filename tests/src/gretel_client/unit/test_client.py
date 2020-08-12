import io
import csv
import json
from unittest.mock import Mock, patch

import pytest
import faker
from faker.providers import misc

from gretel_client import (
    get_cloud_client,
    Client,
    NotFound,
    Unauthorized,
    BadRequest,
    Forbidden,
    project_from_uri,
)
from gretel_client.samplers import ConstantSampler
from gretel_client.readers import CsvReader, JsonReader
from gretel_client.client import DEFAULT_API_ENV_KEY, TIMEOUT


@pytest.fixture
def fake():
    fake = faker.Faker("en_US")
    fake.add_provider(misc)
    return fake


@pytest.fixture()
def client():
    return get_cloud_client("api", "abcd123xyz")


@pytest.fixture
def records():
    chunk1 = [
        {
            "id": "1",
            "data": "foo_1",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        },
        {
            "id": "2",
            "data": "foo_2",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        },
        {
            "id": "3",
            "data": "foo_3",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        },
    ]

    chunk2 = [
        {
            "id": "4",
            "data": "foo_4",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        },
        {
            "id": "1",
            "data": "foo_1",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        },
    ]

    chunk3 = []

    chunk4 = [
        {
            "id": "5",
            "data": "foo_5",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        },
        {
            "id": "6",
            "data": "foo_6",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        },
    ]

    return [
        {"data": {"records": chunk1}},
        {"data": {"records": chunk2}},
        {"data": {"records": chunk3}},
        {"data": {"records": chunk4}},
    ]


@pytest.fixture
def records_rev():
    chunk1 = [
        {
            "id": "1",
            "data": "foo_1",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        },
        {
            "id": "2",
            "data": "foo_2",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        },
        {
            "id": "3",
            "data": "foo_3",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        },
    ]

    chunk2 = [
        {
            "id": "4",
            "data": "foo_4",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        },
        {
            "id": "5",
            "data": "foo_5",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        },
        {
            "id": "6",
            "data": "foo_6",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        },
    ]

    chunk3 = [
        {
            "id": "7",
            "data": "foo_7",
            "metadata": {},
            "ingest_time": "2020-05-10T12:41:55.585538",
        }
    ]

    return [
        {"data": {"records": chunk1}},
        {"data": {"records": chunk2}},
        {"data": {"records": chunk3}},
    ]


@pytest.fixture
def test_input_json_stream(test_records):
    input_json = io.StringIO()
    for record in test_records:
        input_json.write(json.dumps(record) + "\n")
    input_json.seek(0)
    return input_json


@patch("gretel_client.client.Client")
@patch("gretel_client.client.getpass")
@patch("gretel_client.client.os.getenv")
def test_get_cloud_client_prompt(getenv, getpass, Client):
    # when no env is set and prompt is true, ask for gretel key
    getenv.return_value = None
    get_cloud_client("api", "prompt")
    assert getpass.call_count == 1

    # when api key is set, and prompt is true, use api key
    getenv.return_value = "abcd123"
    get_cloud_client("api", "prompt")
    Client.assert_called_with(host="api.gretel.cloud", api_key="abcd123")
    assert getpass.call_count == 1

    # when api key is set and prompt always is true, ask for api key
    get_cloud_client("api", "prompt_always")
    assert getpass.call_count == 2

    # use api key env variable
    get_cloud_client("api", "abc123")
    Client.assert_called_with(host="api.gretel.cloud", api_key="abc123")


@patch("gretel_client.client.get_cloud_client")
@patch("gretel_client.client.getpass")
@patch("gretel_client.client.os.getenv")
def test_get_project_from_uri_prompt(getenv, getpass, gcc):
    test_uri = "gretel://token@api.gretel.cloud/test_proj"
    # when no env is set and prompt is true, ask for gretel key
    getenv.return_value = None
    getpass.return_value = test_uri
    project_from_uri("prompt")
    assert getpass.call_count == 1

    # when api key is set, and prompt is true, use api key
    getenv.return_value = test_uri
    project_from_uri("prompt")
    gcc.assert_called_with("api", "token")
    assert getpass.call_count == 1

    # when api key is set and prompt always is true, ask for api key
    project_from_uri("prompt_always")
    assert getpass.call_count == 2

    # use api key env variable
    gcc.reset_mock()
    project_from_uri(test_uri)
    gcc.assert_called_with("api", "token")


def test_iter_records(records):
    client = get_cloud_client("api", "abc123xyz")
    client._get = Mock(side_effect=records)
    check = []
    for rec in client._iter_records(project="foo"):
        check.append(rec["record"])
        if len(check) == 6:
            break

    assert check == ["foo_3", "foo_2", "foo_1", "foo_4", "foo_6", "foo_5"]


def test_iter_records_reverse(records_rev):
    records = records_rev
    client = get_cloud_client("api", "abcd123xyz")
    client._get = Mock(side_effect=records)
    check = []
    for rec in client._iter_records(project="foo", direction="backward"):
        check.append(rec["record"])

    assert check == ["foo_1", "foo_2", "foo_3", "foo_4", "foo_5", "foo_6", "foo_7"]


def test_iter_records_does_terminate(records):
    client = get_cloud_client("api", "abc123xyz")
    client._get = Mock(side_effect=records)
    for _ in client._iter_records(project="foo", wait_for=1):
        continue


def test_iter_entity_stream_custom_params(records):
    client = get_cloud_client("api", "abc123xyz")
    client._get = Mock(side_effect=records)
    resp = list(
        client._iter_records(
            project="foo",
            entity_stream="test_entity",
            wait_for=1,
            headers={"X-Gretel-Test": "true"},
        )
    )
    assert client._get.called_with(
        "streams/records/foo",
        params={"entity_stream": "test_entity", "flatten": "yes", "with_meta": "yes"},
        headers={"X-Gretel-Test": "true"},
    )


def test_iter_record_with_limit(records):
    client = get_cloud_client("api", "abc123xyz")
    client._get = Mock(side_effect=records)
    check = []
    for rec in client._iter_records(project="foo", record_limit=3):
        check.append(rec)
    assert len(check) == 3


def test_record_writer_csv(fake, client: Client):
    client._post = Mock()
    input_csv = io.StringIO()
    csv_writer = csv.writer(input_csv, quoting=csv.QUOTE_NONNUMERIC)
    header = [f"header_{x}" for x in range(10)]
    csv_writer.writerow(header)
    rows = []
    for _ in range(5):
        row = fake.pylist(nb_elements=10, variable_nb_elements=False)
        csv_writer.writerow(row)

        # CSVs don't preserve types by default. as a result we
        # want to cast everything to a string so that it can be
        # used in the call assertion.
        rows.append([str(val) for val in row])
    input_csv.seek(0)

    client._write_records(project="test-proj", reader=CsvReader(input_csv))

    expected_payload = [dict(zip(header, row)) for row in rows]

    client._post.assert_called_with("records/send/test-proj", data=expected_payload)


def test_json_writer(test_records, test_input_json_stream, client):
    client._post = Mock()
    client._write_records(
        project="test-project", reader=JsonReader(test_input_json_stream)
    )
    client._post.assert_called_with("records/send/test-project", data=test_records)


def test_writer_params_headers(test_records, test_input_json_stream, client):
    client.session.post = Mock()
    client._write_records(
        project="test-project",
        reader=JsonReader(test_input_json_stream),
        params={"detection_mode": "all"},
        headers={"X-Unit-Test": True},
    )
    client.session.post.assert_called_with(
        "https://api.gretel.cloud/records/send/test-project",
        data=json.dumps(test_records),
        params={"detection_mode": "all"},
        timeout=TIMEOUT,
        headers={"X-Unit-Test": True},
    )


def test_write_unauthorized(test_records, client):
    input_json = io.StringIO()
    for record in test_records:
        input_json.write(json.dumps(record) + "\n")
    for _ in range(5000):
        input_json.write(json.dumps({"foo": "bar"}) + "\n")
    input_json.seek(0)
    client._write_record_sync = Mock(
        side_effect=[{}, {}, Unauthorized({"message": "Unauthorized"})]
    )
    check = client._write_records(project="test-project", reader=JsonReader(input_json))
    assert not check
    assert check.api_errors == ["Unauthorized"]


def test_write_badrequest(test_records, client):
    input_json = io.StringIO()
    for record in test_records:
        input_json.write(json.dumps(record) + "\n")
    input_json.seek(0)
    client._write_record_sync = Mock(
        side_effect=BadRequest(
            {"message": "Unauthorized", "context": {"field": ["bad"]}}
        )
    )
    check = client._write_records(project="test-project", reader=JsonReader(input_json))
    assert not check
    assert check.api_errors == ['Unauthorized: {"field": ["bad"]}']


def test_write_forbidden(test_records, client):
    input_json = io.StringIO()
    for record in test_records:
        input_json.write(json.dumps(record) + "\n")
    input_json.seek(0)
    client._write_record_sync = Mock(side_effect=[{}, Forbidden(), {}])
    check = client._write_records(project="test-project", reader=JsonReader(input_json))
    assert check
    assert check.api_errors == []


def test_constant_sampler(fake):
    records = [
        fake.pylist(nb_elements=10, variable_nb_elements=False) for _ in range(100)
    ]
    sampler = ConstantSampler(sample_rate=1)
    sampler.set_source(iter(records))
    assert len(list(sampler)) == 100

    records = [
        fake.pylist(nb_elements=10, variable_nb_elements=False) for _ in range(1000)
    ]
    sampler = ConstantSampler(sample_rate=10)
    sampler.set_source(iter(records))
    count = len(list(sampler))

    # this test is non-deterministic and could transiently
    # fail. statistically it ought not to though.
    assert count > 25 and count < 300


def test_get_project(client: Client):
    client._get = Mock(
        return_value={"data": {"project": {"_id": 123, "description": "", "display_name": ""}}}
    )
    check = client.get_project(name="proj")
    assert check.name == "proj"
    assert check.client == client
    assert check.project_id == 123

    client._create_project = Mock(
        return_value={"data": {"id": "5eb07df99294fd2dbc3dbe6a"}}
    )
    client._get_project = Mock(
        return_value={
            "project": {
                "name": "random",
                "id": "5eb07df99294fd2dbc3dbe6a",
                "description": "",
                "display_name": ""
            }
        }
    )
    check = client.get_project(create=True)
    client._get_project.assert_called_with("5eb07df99294fd2dbc3dbe6a")
    assert check.name == "random"
    assert check.client == client
    assert check.project_id == "5eb07df99294fd2dbc3dbe6a"


class Fake400:
    status_code = 400

    def json(self):
        return {"message": "very bad", "context": {}}


class Fake404(Fake400):
    status_code = 404


class Fake401:
    status_code = 401

    def json(self):
        return {"message": "Unauthorized"}


class Fake403:
    status_code = 403


def test_api_4xx_errors(client: Client):
    client.session.get = Mock(side_effect=[Fake404(), Fake400(), Fake401(), Fake403()])

    with pytest.raises(NotFound):
        client._get("foo", None)

    with pytest.raises(BadRequest):
        client._get("foo", None)

    with pytest.raises(Unauthorized):
        client._get("foo", None)

    with pytest.raises(Forbidden):
        client._get("foo", None)


def test_project_from_uri_bad_string(monkeypatch):
    monkeypatch.delenv(DEFAULT_API_ENV_KEY, raising=False)
    with pytest.raises(ValueError) as err:
        project_from_uri("nope")
    assert "URI must start with" in str(err)

    with pytest.raises(ValueError) as err:
        project_from_uri("gretel://api.gretel.cloud/my_project")
    assert "No API key" in str(err)

    with pytest.raises(ValueError) as err:
        project_from_uri("gretel:/api.gretel.cloud/my_project")
    assert "API hostname" in str(err)

    with pytest.raises(ValueError) as err:
        project_from_uri("gretel://token@api.gretel.cloud/")
    assert "No project" in str(err)

    with pytest.raises(ValueError) as err:
        project_from_uri("gretel://token@api.gretel.cloud")
    assert "No project" in str(err)


def test_project_from_uri_env_api_key(monkeypatch):
    monkeypatch.setenv(DEFAULT_API_ENV_KEY, "abc123")

    with patch("gretel_client.client.get_cloud_client") as mock_get_client:
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        project_from_uri("gretel://api.gretel.cloud/my_project")
        mock_get_client.assert_called_with("api", "abc123")
        mock_client.get_project.assert_called_with(name="my_project")


def test_project_from_uri(monkeypatch):
    with patch("gretel_client.client.get_cloud_client") as mock_get_client:
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        project_from_uri("gretel://tokentoken@api.gretel.cloud/my_project")
        mock_get_client.assert_called_with("api", "tokentoken")
        mock_client.get_project.assert_called_with(name="my_project")

