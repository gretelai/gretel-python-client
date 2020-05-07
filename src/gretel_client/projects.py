"""
High level API for interacting with a Gretel Project
"""
from __future__ import annotations
from typing import TYPE_CHECKING, List, Union
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

        self._iter_records = partial(self.client.iter_records, project=self.name)

        self._get_field_count = partial(
            self.client.get_fields,
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
        return self._iter_records(**kwargs)

    def flush(self):
        """This will flush all project data from the Gretel metastore.

        This includes all Field, Entity, and cached Record information

        NOTE: This command runs asyncronously. When it returns, it
        means it has only triggered the flush operation in Gretel. This
        full operations may take several seconds to complete.
        """
        self.client.flush_project(self.project_id)

    def send_records(self, data: Union[List[dict], dict]):
        reader = JsonReader(data)
        return self.client.write_records(
            project=self.name,
            reader=reader
        )

    def send_dataframe(self, df: pd.DataFrame):
        reader = DataFrameReader(df)
        self.client.write_records(
            project=self.name,
            reader=reader
        )

    def head(self, n: int = 5):
        """Get the top N records, flattened,
        and return them as a DataFrame. This
        mimics the DataFrame.head() method

        Args:
            n: the number of records to retrieve
        """
        recs = self.client._get_records_sync(
            self.name, 
            {'flatten': 'yes', 'count': n}
        )
        recs = [item['data'] for item in recs]
        return pd.DataFrame(recs)

    def sample(self):
        return self.client._get_records_sync(
            self.name,
            {'with_meta': 'yes'}
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
