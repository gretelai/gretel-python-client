import csv
from typing import List

import pytest
import faker
from faker.providers import misc

from gretel_client.client import Client, get_cloud_client


@pytest.fixture
def fake():
    fake = faker.Faker('en_US')
    fake.add_provider(misc)
    return fake

@pytest.fixture
def client() -> Client:
    return get_cloud_client('api', 'abcd123xyz')


@pytest.fixture
def test_records(fake) -> List[dict]:
    records  = []
    headers = [f'header_{x}' for x in range(10)]
    for _ in range(5):
        row = [fake.pystr(), fake.pystr(), fake.pyfloat(), fake.pyint(),
               fake.pystr()]
        records.append({k: v for k, v in zip(headers, row)})
    return records

@pytest.fixture
def generate_csv():
    def _(test_records, stream):
        csv_writer = csv.writer(stream, quoting=csv.QUOTE_NONNUMERIC,
                                delimiter=',')
        csv_writer.writerow(test_records[0].keys())
        for record in test_records:
            csv_writer.writerow(record.values())
        return test_records
    return _
