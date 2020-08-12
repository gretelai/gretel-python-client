"""
High level API for interacting with a Gretel Project
"""
from typing import TYPE_CHECKING, List, Union, Tuple
from functools import partial
from copy import deepcopy

from gretel_client.readers import JsonReader, DataFrameReader


try:
    import pandas as pd
    from pandas import DataFrame as _DataFrameT
except ImportError:  # pragma: no cover
    pd = None

    class _DataFrameT:
        ...  # noqa


if TYPE_CHECKING:  # pragma: no cover
    from gretel_client.client import Client, WriteSummary

DEFAULT_DETECTION_MODE = "fast"


class Project:
    """Representation of a single Gretel project. In general you should not have
    to init this class directly, but can make use of the factory method from a
    ``Client`` instance.

    Using the factory method::

        from gretel_client import get_cloud_client

        client = get_cloud_client('api', 'your_api_key')
        project = client.get_project(create=True)  # creates a project with an auto-named slug


    **Customizing the request**

    The ``Project`` class wraps Gretel's REST api under the hood. While most query params
    are represented as named keyword arguments, it's possible to pass in custom query
    parameters via ``params``, or http headers via ``headers``.

    For example::

        project.iter_records(project="test", params={"regex_query": "(cc|credit_card)"})

    """

    def __init__(
        self,
        *,
        name: str,
        client: "Client",
        project_id: str,
        desc: str = None,
        display_name: str = None,
    ):
        self.name = name
        """The unique name of the project. This is either set by you or auto
        managed by Gretel
        """

        self.client = client  # type: Client

        self.project_id = project_id
        """The unique Project ID for your project. This is auto-managed by Gretel
        """

        self.description = desc
        """A short description of the project
        """

        self.display_name = display_name
        """The main display name used in the Gretel Console for your project
        """

        self._iter_records = partial(self.client._iter_records, project=self.name)

        self._get_field_count = partial(
            self.client._get_fields, self.name, detail="no", count=1
        )

    @property
    def field_count(self) -> int:
        """Return the total number of fields (as an int) in
        the project.
        """
        return self._get_field_count()["total_field_count"]

    @property
    def record_count(self) -> int:
        """Return the total number of records that have been
        ingested (as an int) for the project.
        """
        return self._get_field_count()["project_record_count"]

    @property
    def entities(self) -> List[dict]:
        """Return all entities that have been observed in
        this project
        """
        return self.client._get_entities(self.name)

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
            entity_stream: Return a record stream that only contains records
                containing a specifc entity label.
            headers: (dict) Define additional http request headers.
            params: (dict) Pass custom request query parameters.

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

    def flush(self, **kwargs):
        """This will flush all project data from the Gretel metastore.

        This includes all Field, Entity, and cached Record information

        NOTE:
            This command runs asynchronously. When it returns, it
            means it has only triggered the flush operation in Gretel. This
            full operations may take several seconds to complete.
        """
        self.client._flush_project(self.project_id)

    def send(
        self,
        data: Union[List[dict], dict],
        detection_mode: str = DEFAULT_DETECTION_MODE,
        **kwargs,
    ) -> Tuple[list, list]:
        """Write one or more records synchronously. This is similar
        to making a single API call to the records endpoint. You will also
        receive the success and failure arrays back which contain the Gretel IDs
        that were generated for each ingested record.

        NOTE:
            Because this is just like making a single call to the Records
            endpoint, the maximum record count per-call will be enforced.

        Args:
            data: a dict or a list of dicts
            detection_mode: Determines how to route the record through Gretel's entity
                detection pipeline. Valid options include "fast", "all" and "none".
                Selecting "all" will pass the records through a ML/NLP pipeline, but
                may increase processing latency. Selecting "none" will skip the
                detection pipeline entirely.
            headers: (dict) Define additional http request headers.
            params: (dict) Pass custom request query parameters.

        Returns:
            A tuple of (success, failure) lists
        """
        kwargs = self._patch_params(kwargs, "detection_mode", detection_mode)
        ret = self.client._write_record_sync(self.name, data, **kwargs)["data"]
        return ret["success"], ret["failure"]

    def _patch_params(self, kwargs: dict, key: str, value: str) -> dict:
        """Patch in named arguments into query param **kwarg type dictionary"""
        if not isinstance(kwargs, dict):
            raise ValueError("kwargs must be a dictionary")
        if "params" in kwargs:
            kwargs["params"][key] = value
        else:
            kwargs["params"] = {key: value}
        return kwargs

    def send_bulk(
        self,
        data: Union[List[dict], dict],
        detection_mode: str = DEFAULT_DETECTION_MODE,
        **kwargs,
    ) -> "WriteSummary":
        """Send a dict or list of dicts to the project.  Records
        are queued and sent in parallel for performance. API
        responses are not returned.

        NOTE:
            Since a queue and threading is used here, you
            can send any number of records in the ``data`` param.
            The records will automatically be chunked up into
            appropriately sized buffers to send to the Records API.

        Args:
            data: A dict or a list of dicts.
            detection_mode: Determines how to route the record through Gretel's entity
                detection pipeline. Valid options include "fast", "all" and "none".
                Selecting "all" will pass the record through a ML/NLP pipeline, but
                may increase processing latency. Selecting "none" will skip the
                entity detection pipeline entirely.
            headers: (dict) Pass in custom http headers to the request.
            params: (dict) Pass custom query parameters to the request.
        Returns:
            A ``WriteSummary`` instance.
        """
        reader = JsonReader(data)
        kwargs = self._patch_params(kwargs, "detection_mode", detection_mode)
        return self.client._write_records(project=self.name, reader=reader, **kwargs)

    def send_dataframe(
        self,
        df: _DataFrameT,
        detection_mode: str = DEFAULT_DETECTION_MODE,
        sample=None,
        **kwargs,
    ) -> "WriteSummary":
        """Send the contents of a DataFrame

        This will convert each row of the DataFrame
        into a dictionary and send it as a record.  This
        operation happens using the bulk writer so no
        results from the API calls are returned.

        Args:
            df: A pandas DataFrame
            sample: Specify a subset of the DataFrame rows to be sent. If ``sample``
                is > 1, then ``sample`` number of rows will be queued for sending. If
                ``sample`` is between 0 and 1, then a fraction of the DataFrame's rows
                will be queued for sending. So a ``sample`` of .5 will queue up half
                of the DataFrame's rows.
            detection_mode: Determines how to route the record through Gretel's entity
                detection pipeline. Valid options include "fast", "all" and "none".
                Selecting "all" will pass the records through a ML/NLP pipeline, but
                may increase processing latency. Selecting "none" will skip the
                detection pipeline entirely.
            headers: (dict) Pass in custom http headers to the request.
            params: (dict) Pass custom query parameters to the request.

        NOTE:
            Sampling is randomized, not done by first N.

        Returns:
            An instance of ``WriteSummary``

        Raises:
            ``RuntimeError`` if Pandas is not installed
            ``ValueError`` if a Pandas DataFrame was not provided
        """
        if not pd:  # pragma: no cover
            raise RuntimeError("pandas must be installed for this feature")

        if not isinstance(df, pd.DataFrame):  # pragma: no cover
            raise ValueError("A Pandas DataFrame is required!")

        new_df = df

        if sample is not None:
            if sample <= 0:
                raise ValueError("Sample must be greater than 1")
            elif sample < 1:
                new_df = df.sample(frac=sample)
            else:
                if sample > len(df):
                    raise ValueError("Sample size cannot be larger than DataFrame")
                new_df = df.sample(n=sample)

        reader = DataFrameReader(new_df)
        kwargs = self._patch_params(kwargs, "detection_mode", detection_mode)
        return self.client._write_records(project=self.name, reader=reader, **kwargs)

    def head(self, n: int = 5) -> _DataFrameT:
        """Get the top N records, flattened,
        and return them as a DataFrame. This
        mimics the DataFrame.head() method

        Args:
            n: the number of records to retrieve

        Returns a Pandas DataFrame
        """
        if not pd:  # pragma: no cover
            raise RuntimeError("pandas must be installed to use this feature")
        recs = self.client._get_records_sync(
            self.name, params={"flatten": "yes", "count": n}
        )
        recs = [item["data"] for item in recs]
        return pd.DataFrame(recs)

    def sample(self, n=10) -> List[dict]:
        """Return the top N records. These records
        will be in the raw format that they were received
        and will have all Gretel metadata attached.

        Returns a list that matches the response
        from the REST API.

        NOTE:
            The outter keys from the API response
            are removed and the list of records is only returned
        """
        return self.client._get_records_sync(
            self.name, params={"with_meta": "yes", "count": n, "flatten": "yes"}
        )

    def get_field_details(self, *, entity: str = None, count=500) -> List[dict]:
        """Return details for all fields in the project.

        Args:
            entity: if an entity label is supplied, then
                only return fields that contain that entity
            count: how many fields to retrieve

        Returns:
            A list of dictionaries that match the Fields API
            schema from the Gretel REST API
        """
        field_filter = None
        if entity:
            field_filter = {"filter": "entity", "value": entity}
        return self.client._get_fields(
            self.name, count=count, field_filter=field_filter
        )["data"]["fields"]

    def get_field_entities(
        self, *, as_df=False, entity: str = None
    ) -> Union[List[dict], _DataFrameT]:
        """Download all fields from the Metastore and create
        flat rows of all field + entity relationships.

        Normally, the list of all entities for a given field is
        stored in an array attached to the field level, here we
        will de-normalize this and create a single record
        for each field and entity combination.

        So if a field called "foo" has 3 entities embedded inside
        its metadata, we'll create 3 new rows out of this field
        metadata. We can then easily return this as a DataFrame.

        Args:
            as_df: Return this dataset as a Pandas DataFrame
            entity: Filter on a specific entity, if None, we'll
                use all fields

        Returns:
            A Pandas DataFrame or a list of dicts
        """
        field_meta = self.get_field_details(entity=entity)
        recs = []
        for field in field_meta:
            # if there are no entities, skip
            if not field["entities"]:
                continue

            # remove the types
            field.pop("types")

            # store the entities
            ents = field.pop("entities")

            for ent in ents:
                # make a clean copy of the current
                # top level field data
                tmp = deepcopy(field)
                for k, v in ent.items():
                    tmp[f"entity_{k}"] = v
                recs.append(tmp)

        if not as_df:  # pragma: no cover
            return recs
        else:
            if not pd:  # pragma: no cover
                raise RuntimeError("cannot export as a DF without pandas installed")
            return pd.DataFrame(recs)

    def delete(self):
        """Deletes this project. After this is called, this
        object can be discarded or deleted itself.

        NOTE:
            If you attempt to use other methods on this project
            instance after deletion, you will receive API errors.
        """
        self.client._delete_project(self.project_id)
