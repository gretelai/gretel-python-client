import pytest
import io
import json
from collections import namedtuple
from pathlib import Path

import pandas as pd

from gretel_client.readers import CsvReader, JsonReader, try_data_source, DataFrameReader
from gretel_client.cli import SeekableStreamBuffer


def test_csv_reader_sniffer(generate_csv, test_records):
    input_csv = io.StringIO()
    generate_csv(test_records, input_csv)
    input_csv.seek(0)
    reader = CsvReader(input_csv)

    for expected, actual in zip(test_records, reader):
        assert actual == {k: str(v) for k, v in expected.items()}

def test_csv_file_reader(generate_csv, test_records, tmpdir_factory):
    file_path = tmpdir_factory.mktemp('test') / 'test_csv.csv'
    with open(file_path, 'w') as input_csv:
        generate_csv(test_records, input_csv)

    reader = CsvReader(file_path)

    for expected, actual in zip(test_records, reader):
        assert actual == {k: str(v) for k, v in expected.items()}

    with pytest.raises(StopIteration):
        next(reader) # call next one more time to ensure we get a StopIteration
        assert reader.data_source.closed  # type: ignore

    reader = CsvReader(file_path, sniff=False)

    for expected, actual in zip(test_records, reader):
        assert actual == {k: str(v) for k, v in expected.items()}

    with pytest.raises(StopIteration):
        next(reader) # call next one more time to ensure we get a StopIteration
        assert reader.data_source.closed  # type: ignore

def test_json_buffer_reader(test_records, tmpdir_factory):
    input_json = io.StringIO()
    json_file = tmpdir_factory.mktemp('test') / 'test_json.json'

    with open(json_file, 'w') as file_handle:
        for record in test_records:
            json_record = json.dumps(record) + '\n'
            input_json.write(json_record)
            file_handle.write(json_record)
    input_json.seek(0)

    reader = JsonReader(input_json)
    for expected, actual in zip(test_records, reader):
        assert actual == expected

    reader = JsonReader(str(json_file))
    for expected, actual in zip(test_records, reader):
        assert actual == expected

    with pytest.raises(StopIteration):
        next(reader) # call next one more time to ensure we get a StopIteration
        assert reader.data_source.closed  # type: ignore

    # load a single dict
    reader = JsonReader({'foo': 'bar'})
    assert next(reader) == {'foo': 'bar'}
    with pytest.raises(StopIteration):
        next(reader)

def test_json_file_array(test_records, tmpdir_factory):
    json_file = tmpdir_factory.mktemp('test') / 'test_json.json'
    with open(json_file, 'w') as file_handle:
        file_handle.write(json.dumps(test_records))

    reader = JsonReader(json_file)
    for expected, actual in zip(test_records, reader):
        assert actual == expected


def test_object_reader(fake, test_records):
    reader = JsonReader(test_records)
    for expected, actual in zip(test_records, reader):
        assert actual == expected

    Test = namedtuple('Test', 'one two')
    test_objects = []
    for _ in range(5):
        test_objects.append(Test(fake.pystr(), fake.pystr()))

    mapper = lambda record: {'one': record.one, 'two': record.two}
    reader = JsonReader(test_objects, mapper=mapper)
    for expected, actual in zip(test_objects, reader):
        assert actual == mapper(expected)



def test_data_source_reader(tmpdir_factory):
    json_file = tmpdir_factory.mktemp('test') / 'test_json.json'
    json_file_path = Path(json_file)
    json_file_path.touch()

    assert isinstance(try_data_source(json_file), io.TextIOWrapper)
    assert isinstance(try_data_source(json_file_path), io.TextIOWrapper)

    with open(json_file) as fd:
        assert isinstance(try_data_source(fd), io.TextIOWrapper)

    with pytest.raises(FileNotFoundError):
        try_data_source('not_found.json')

    input_io = io.StringIO()
    assert isinstance(try_data_source(input_io), io.StringIO)

    input_object = [{'tes_record': True}]
    assert isinstance(try_data_source(input_object), list)


    stdin = SeekableStreamBuffer(io.BytesIO())
    assert isinstance(try_data_source(stdin), SeekableStreamBuffer)


def test_empty_input_stream():
    input_stream = io.StringIO()
    reader = JsonReader(input_stream)
    assert len(list(reader)) == 0

    reader = CsvReader(input_stream)
    assert len(list(reader)) == 0


def test_dataframe_reader():
    df = pd.DataFrame([
        {'foo': 'bar'},
        {'foo': 'bar2'},
        {'foo': 'bar3'}
    ])
    reader = DataFrameReader(df)
    check = []
    for row in reader:
        check.append(row)
    assert check == [{'foo': 'bar'}, {'foo': 'bar2'}, {'foo': 'bar3'}]