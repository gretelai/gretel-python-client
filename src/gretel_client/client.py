import json
from functools import wraps
import time
from typing import Iterator, Callable, Optional, Union, List
import threading
from queue import Queue
from getpass import getpass

import requests

from gretel_client.readers import Reader
from gretel_client.samplers import ConstantSampler, Sampler, \
    get_default_sampler
from gretel_client.projects import Project
import gretel_client.pkg_installers as pkg


TIMEOUT = 30
RECORD = 'record'
RECORDS = 'records'
META = 'metadata'
DATA = 'data'
ID = 'id'
INGEST_TIME = 'ingest_time'
PROMPT = 'prompt'

MAX_BATCH_SIZE = 50


def get_cloud_client(stage: str, api_key: str):
    return Client(
        host=f'{stage}.gretel.cloud',
        api_key=getpass('Enter Gretel API key: ') if api_key == PROMPT else api_key,
        ssl=True
    )


class ClientError(Exception):
    pass


class BadRequest(ClientError):
    """A custom error class that can be raised when a non-200 OK
    is received back from the Gretel API.

    The Gretel API returns errors with the following format::

        {
            'message': 'a description of what is wrong
            'context': {}
        }

    The ``context`` key will contain structured information about
    a particular field if there was an error with it.
    """

    def __init__(self, msg: dict):
        """Create an API exception.

        Args:
            msg: A dictionary that is created from the JSON payload
                returned by the Gretel API
        """
        self._msg_dict = msg
        self.message = self._msg_dict['message']
        self.context = self._msg_dict['context']

    def __str__(self):
        return self.message


class NotFound(BadRequest):
    pass


def _api_call(method):
    def dec(func):
        @wraps(func)
        def handler(inst, *args, **kwargs):
            try:
                res: requests.Response
                res = func(inst, *args, **kwargs)
            except requests.HTTPError as err:  # pragma: no cover
                raise ClientError(f'HTTP error: {str(err)}') from err

            if res.status_code != 200:
                if res.status_code == 404:
                    raise NotFound(res.json())
                raise BadRequest(res.json())

            if method in ('get', 'post', 'delete'):
                return res.json()
        return handler
    return dec


class Client:

    def __init__(self, *, host, api_key=None, ssl=True):
        self.host = host
        self.api_key = api_key
        self.ssl = ssl

        self.base_url = None
        self.session = None

        self._build_base_url()
        self._build_session()

    def _build_base_url(self):
        path = f'{self.host}/'
        if self.ssl:
            self.base_url = 'https://' + path
        else:
            self.base_url = 'http://' + path  # pragma: no cover

    def _build_session(self):
        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({'Authorization': self.api_key})

    @_api_call('get')
    def _get(self, resource, params):
        return self.session.get(
            self.base_url + resource, params=params, timeout=TIMEOUT
        )

    @_api_call('post')
    def _post(self, resource, params: dict, data: dict):
        if data is not None:
            data = json.dumps(data)
        return self.session.post(
            self.base_url + resource, params=params, data=data,
            timeout=TIMEOUT
        )

    @_api_call('delete')
    def _delete(self, resource):
        return self.session.delete(self.base_url + resource, timeout=TIMEOUT)

    def _get_entities(self, project):
        ents = self._get(f'fields/entities/{project}', None)
        return ents

    def _get_fields(self, project, detail='yes', count=500, field_filter: dict = None):
        params = {'detail': detail, 'count': count}
        if field_filter:
            params.update(field_filter)
        fields = self._get(f'fields/schema/{project}', params=params)
        return fields

    def _create_project(self, name=None, desc=None):
        """
        NOTE(jm): not supporting display name or long description
        here yet
        """
        payload = {}
        if name is not None:
            payload['name'] = name
        if desc is not None:
            payload['description'] = desc

        return self._post(f'projects', None, data=payload)

    def _delete_project(self, project_id):
        return self._delete(f'projects/{project_id}')

    def _flush_project(self, project_id: str):
        return self._post(f'projects/{project_id}/flush', None, None)

    def _write_record_sync(self, project: str, records: Union[list, dict]):
        """Write a batch of records to Gretel's API

        Args:
            project: Target Gretel project identifier.
            records: An array of records to write to the api.
        """
        if isinstance(records, dict):
            records = [records]
        return self._post(f'records/send/{project}', {}, records)

    def _write_record_batch(self, project, write_queue, error_queue):
        while True:
            record_batch = write_queue.get()
            try:
                self._write_record_sync(project, record_batch)
            except Exception as e:
                print(e)
                error_queue.push(record_batch)
            write_queue.task_done()

    def _write_records(
        self,
        *,
        project: str,
        reader: Reader,
        sampler: Sampler = get_default_sampler(),
        worker_threads: int = 10,
        max_inflight_batches: int = 100,
        disable_progress: bool = False,
        use_progress_widget: bool = False
    ):
        """
        Write a stream of input records to Gretel's API.

        TODO (dn): make `reader` param optional and instead try to
            classify the data based on a set of reader specific
            heuristics.

        Args
            project: Target project to write records to.
            input_records: Records to write to project.
            reader: A strategy for parsing and reading the input data.
                Currently only csv inputs are supported.
            sampler: Record sampling strategy. By default all records
                are written to the API.
            worker_threads: Determines how many worker threads are spun
                up to manage write requests.
            max_inflight_batches: Determines how many record batches to
                hold in memory before blocking and applying backpressure
                on the record iterator.
            disable_progress: If set to `true` it will disable the progress
                indicator.
            use_progress_widget: For use inside a jupyter notebook
                environment. If set to `true` it will try and use tqdm's
                ipywidget instead of a text based progress indicator.
        """
        sampler.set_source(iter(reader))
        write_queue = Queue(max_inflight_batches)  # backpressured worker queue
        error_queue = Queue()

        threads = []
        for _ in range(worker_threads):
            t = threading.Thread(target=self._write_record_batch,
                                 args=(project, write_queue, error_queue),
                                 daemon=True)
            t.start()
            threads.append(t)

        if use_progress_widget:
            from tqdm.notebook import tqdm
        else:
            from tqdm import tqdm

        t = tqdm(total=1, unit='records', disable=disable_progress)
        acc = []
        for record in sampler:
            acc.append(record)
            if len(acc) == MAX_BATCH_SIZE:
                write_queue.put(acc)
                acc = []
            t.update()
        if acc:  # flush remaining records from accumulator
            write_queue.put(acc)
            t.update()
        t.close()
        write_queue.join()

    def _build_callback(self, post_process):
        if callable(post_process):
            callback = post_process
        else:
            def noop(x): return x
            callback = noop
        return callback

    def _get_records_sync(self, project: str, params: dict, count: int = None):
        if count is not None:
            params['count'] = count
        return self._get(f'streams/records/{project}', params) \
                   .get(DATA) \
                   .get(RECORDS, [])

    def __iter_records(
        self,
        project: str,
        position: Optional[str] = None,
        post_process: Callable = None,
        direction: str = 'forward',
        wait_for: int = -1,
    ):
        if direction not in ('forward', 'backward'):
            raise AttributeError('direction parameter is invalid.')
        callback = self._build_callback(post_process)
        forward = direction == 'forward'
        start_key = 'oldest' if forward else 'newest'
        records_seen = 0
        last = position
        stream_start_time = time.time()
        while True:
            if wait_for > 0 and time.time() - stream_start_time > wait_for:
                return
            params = {'with_meta': 'yes', 'flatten': 'yes'}
            if last is not None:
                params.update({start_key: last})
            # print(self._get(f'streams/records/{project}', params))
            records = self._get_records_sync(project, params)

            # if we're going backwards we'll eventually
            # hit a point when we just only keep getting
            # one record back, since the id will just keep
            # being reset to that id
            if not forward and len(records) == 1:
                # yield the final record
                yield callback({
                    RECORD: records[0][DATA],
                    META: records[0][META],
                    INGEST_TIME: records[0][INGEST_TIME]
                })
                return

            if not records:
                if forward:
                    continue
                else:
                    return
            next_last = records[0][ID] if forward else records[-1][ID]
            record_iterator = reversed(records) if forward else records
            for record in record_iterator:
                if forward:
                    if record[ID] == last:
                        continue
                yield callback({
                    RECORD: record[DATA],
                    META: record[META],
                })
                records_seen += 1
            last = next_last
            if forward:
                time.sleep(.5)

    def _iter_records(
        self,
        *,
        project: str,
        position: Optional[str] = None,
        post_process: Callable = None,
        direction: str = 'forward',
        record_limit: int = -1,
        wait_for: int = -1,
    ) -> Iterator[dict]:
        """
        Provides an iterator for moving backward or forward
        in a project's record stream.

        Args:
            project: Project name to fetch records from
            position: Record id that determines stream starting point.
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

        Raises:
            TypeError: When invalid `direction` parameter supplied.
        """
        record_source = self.__iter_records(
            project, position, post_process, direction, wait_for)
        record_iterator = ConstantSampler(record_limit=record_limit)
        record_iterator.set_source(record_source)
        return record_iterator

    def _get_project(self, name):
        return self._get(f'projects/{name}', None)['data']

    def search_projects(self, count=200, query=None) -> List[Project]:
        """Search for projects that you are a member of.

        Args:
            count: the max number of projects to return
            query: an optional query string to filter projects on

        Returns:
            A list of ``Project`` instances
        """
        params = {'limit': count}
        if query is not None:
            params.update({'query': query})
        projects = self._get('projects', params=params)['data']['projects']
        return [Project(name=p['name'], client=self, project_id=p['_id']) for p in projects]

    def _create_get_project(self, name=None):
        res = self._create_project(name=name)
        _id = res['data']['id']
        p = self._get_project(_id)['project']
        return Project(name=p['name'], client=self, project_id=_id)

    def get_project(self, *, name=None, create=False) -> Project:
        """
        TODO: docstrings
        """
        if name:
            try:
                p = self._get_project(name)['project']
                return Project(name=name, client=self, project_id=p['_id'])
            except NotFound:
                if create:
                    return self._create_get_project(name=name)
                else:
                    raise

        if not name and create:
            return self._create_get_project()

    def install_transformers(self):
        """Installs the latest version of the Gretel Tranfsormers package'
        """
        pkg.install_transformers(self.api_key, self.host)
