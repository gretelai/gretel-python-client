import io
import pytest
import sys
import csv
from unittest.mock import Mock, patch

import faker
from faker.providers import misc

from gretel_client import get_cloud_client
from gretel_client import cli
from gretel_client.readers import CsvReader, JsonReader

from io import TextIOWrapper
import json

@pytest.fixture(autouse=True)
def client():
    return get_cloud_client('api', 'abcd123xyz')


def test_cli_write(client, generate_csv, test_records, tmpdir_factory):
    file_path = tmpdir_factory.mktemp('test') / 'test_csv.csv'
    with open(file_path, 'w') as input_csv:
        generate_csv(test_records, input_csv)

    sys.argv = ['gretel', '--project', 'test-proj', 'write', '--file',
                str(file_path), '--sample-rate', '2', '--max-record', '10']
    client.write_records = Mock()
    parser = cli.parse_command()
    command = parser.parse_args()
    command.func(command, client)

    _, kwargs = client.write_records.call_args

    assert kwargs['project'] == 'test-proj'
    assert isinstance(kwargs['reader'], CsvReader)
    assert kwargs['sampler'].sample_rate == 2
    assert kwargs['sampler'].record_limit == 10


def test_cli_write_json_stream(client, test_records):

    class TestInput:
        def __init__(self):
            self.input_buffer = io.BytesIO()
        @property
        def buffer(self):
            return self.input_buffer

    sys.stdin = TestInput()

    for record in test_records:
        sys.stdin.buffer.write(f"{json.dumps(record)}\n\n".encode())
    sys.stdin.buffer.seek(0)

    sys.argv = ['gretel', '--project', 'test-proj', 'write', '--stdin',
                '--reader', 'json']

    client._post = Mock()
    parser = cli.parse_command()
    command = parser.parse_args()
    command.func(command, client)

    client._post.called_with('records/send/test-proj', {}, test_records)



def test_cli_tail(client):
    sys.argv = ['gretel', '--project', 'test-proj', 'tail']
    client.iter_records = Mock(return_value=[])
    parser = cli.parse_command()
    command = parser.parse_args()
    command.func(command, client)

    assert client.iter_records.call_count == 1
