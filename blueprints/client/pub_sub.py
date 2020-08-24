"""
Using both records and stream API endpoints to write / read from
a Gretel Project in near real-time. This script will create
a temporary project and send fake records and continuously
consume the labeled records.

Usage::

    python pub_sub.py YOUR_API_KEY
"""
import sys
import threading
import time

from faker import Faker

from gretel_client import get_cloud_client
from gretel_client.client import temporary_project
from gretel_client.projects import Project


def subscriber(project: Project):
    # By default, ``iter_records()`` will block waiting for
    # new labeled records to be ready for consumption. It will
    # fetch in batches, so if there are already records in the
    # stream, we will immediately yield the most recent 200 records.
    for rec in project.iter_records():
        yield rec


def publish(project: Project, event: threading.Event):
    fake = Faker()
    while not event.is_set():
        rec = {
            "name": fake.name(),
            "phone": fake.phone_number()
        }
        project.send(rec)
        time.sleep(1)


def start(api_key: str):
    client = get_cloud_client("api", api_key)
    with temporary_project(client) as project:
        publish_event = threading.Event()
        publish_thread = threading.Thread(target=publish, args=(project, publish_event))
        publish_thread.start()
        try:
            for rec in subscriber(project):
                print(rec)
        except KeyboardInterrupt:
            print("Shutting down...")
        finally:
            publish_event.set()
            publish_thread.join()


if __name__ == "__main__":
    start(sys.argv[1])
