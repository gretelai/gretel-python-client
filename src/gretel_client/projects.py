"""
High level API for interacting with a Gretel Project
"""
from __future__ import annotations
from typing import TYPE_CHECKING, List, Union, Tuple
from functools import partial
import pandas as pd

from gretel_client.readers import JsonReader, DataFrameReader


# Avoid circular import
if TYPE_CHECKING:
    from gretel_client.client import Client


class Project:

    def __init__(self, *, name: str, client: Client, project_id: str):
        self.name = name
        self.client = client  # type: Client
        self.project_id = project_id

        self._iter_records = partial(self.client._iter_records, project=self.name)

        self._get_field_count = partial(
            self.client._get_fields,
            self.name,
            detail='no',
            count=1
        )

    @property
    def field_count(self):
        """Return the total number of fields (as an int) in
        the project
        """
        return self._get_field_count()['total_field_count']

    @property
    def record_count(self):
        """Return the total number of records that have been
        ingested (as an int) for the project
        """
        return self._get_field_count()['project_record_count']

    def iter_records(self, **kwargs):
        """Iterate forwards (optionally waiting) or backwards in
        the record stream.

        Args:
            position: Record ID that determines stream starting point.
            post_process: A function to apply against incoming records.
                This is useful for applying record transformations.
            direction: Determine what direction  in time to move across a
                stream. Valid options include `forward` or `backward`.
            record_limit: The number of records to iterate before
                terminating the iterator. If `record_limit` is less
                than zero, the iterator will continue forward in time
                indefinitely or backwards until the last record is reached.
                If the iterator is moving forward in time, and there are no
                new records on the stream, the function will block until
                more records become available.
            wait_for: Time in seconds to wait for new records to arrive
                before closing the iterator. If the number is set to a
                value less than 0, the iterator will wait indefinitely.

        Yields:
            An individual record object. If no `record_limit` is passed and
            the iterator is moving forward in time, the function will loop
            indefinitely waiting for new records. During this time, the
            function will block until new records become available. If
            the iterator is moving backwards (or historically) through a
            stream, the iterator will continue until the `record_limit` is
            reached, or until the first record in the stream is found.
        """
        return self._iter_records(**kwargs)

    def flush(self):
        """This will flush all project data from the Gretel metastore.

        This includes all Field, Entity, and cached Record information

        NOTE: This command runs asyncronously. When it returns, it
        means it has only triggered the flush operation in Gretel. This
        full operations may take several seconds to complete.
        """
        self.client._flush_project(self.project_id)

    def send(self, data: Union[List[dict], dict]) -> Tuple[list, list]:
        """Write one or more records syncronously.

        Args:
            data: a dict or a list of dicts

        Returns:
            A tuple of (success, failure) lists
        """
        ret = self.client._write_record_sync(self.name, data)['data']
        return ret['success'], ret['failure']

    def send_async(self, data: Union[List[dict], dict]):
        """Send a dict or list of dicts to the project.  Records
        are queued and send in parallel for performance. API
        reponses are not returned

        Args:
            data: a dict or a list of dicts
        """
        reader = JsonReader(data)
        return self.client._write_records(
            project=self.name,
            reader=reader
        )

    def send_dataframe(self, df: pd.DataFrame):
        """Send the contents of a DataFrame

        This will convert each row of the DataFrame
        into a dictionary and send it as a record.  This
        operation happens using the async writer so no
        results from the API calls are returned
        """
        reader = DataFrameReader(df)
        self.client.write_records(
            project=self.name,
            reader=reader
        )

    def head(self, n: int = 5) -> pd.DataFrame:
        """Get the top N records, flattened,
        and return them as a DataFrame. This
        mimics the DataFrame.head() method

        Args:
            n: the number of records to retrieve

        Returns a Pandas DataFrame
        """
        recs = self.client._get_records_sync(
            self.name,
            {'flatten': 'yes', 'count': n}
        )
        recs = [item['data'] for item in recs]
        return pd.DataFrame(recs)

    def sample(self, n=10) -> list:
        """Return the top N records. These records
        will be in the raw format that they were received
        and will have all Gretel metadata attached.

        Returns a list that matches the response
        from the REST API.

        NOTE: The outter keys from the API response
        are removed and the list of records is only returned
        """
        return self.client._get_records_sync(
            self.name,
            {'with_meta': 'yes', 'count': n}
        )

    def get_field_details(self, *, entity: str = None) -> List[dict]:
        """Return details for all fields in the project.

        Args:
            entity: if an entity label is supplied, then
                only return fields that contain that entity

        Returns:
            A list of dictionaries that match the Fields API
            schema from the Gretel REST API

        NOTE: We don't do any automatic pagination here yet. So we'll
        actually make an API call to first find the number of fields
        in the project, and use that as the ``count`` parameter to the
        Fields API call
        """
        field_filter = None
        if entity:
            field_filter = {'filter': 'entity', 'value': entity}
        return self.client.get_fields(
            self.name, count=self.field_count, field_filter=field_filter
        )['data']['fields']

    def delete(self):
        """Deletes this project. After this is called, this
        object can be discarded or deleted itself.

        NOTE: If you attempt to use other methods on this project
        instance after deletion, you will receive API errors.
        """
        self.client._delete_project(self.project_id)
