import json
import os
import sys

from pathlib import Path
from typing import Dict, Optional, Union

import click

from gretel_client.cli.common import poll_and_print, SessionContext
from gretel_client.cli.utils.parser_utils import ref_data_factory
from gretel_client.config import get_logger, get_session_config
from gretel_client.models.config import get_model_type_config, GPU
from gretel_client.projects.common import WAIT_UNTIL_DONE
from gretel_client.projects.docker import ContainerRun, ContainerRunError
from gretel_client.projects.jobs import Job
from gretel_client.projects.models import Model
from gretel_client.projects.records import RecordHandler

log = get_logger(__name__)


class _PythonSessionContext(click.Context):
    """CLI context duck"""

    def __init__(self):
        ...

    def exit(self, code):
        sys.exit(code)

    @property
    def invoked_subcommand(self):
        return None


def poll(job: Job, wait: int = WAIT_UNTIL_DONE):
    """Polls a ``Model`` or ``RecordHandler``.

    Args:
        job: The job to poll
        wait: The time to wait for the job to complete.
    """
    sc = SessionContext(_PythonSessionContext(), "json")
    sc.log.info("Starting poller")
    descriptions = get_description_set(job)
    if not descriptions:
        raise ValueError("Cannot fetch Job polling descriptions")
    sc.print(data=job.print_obj)
    poll_and_print(job, sc, job.runner_mode, descriptions, wait)


def get_description_set(job: Job) -> Optional[dict]:
    model_type_config = get_model_type_config(job.model_type)
    if isinstance(job, Model):
        return model_type_config.train_status_descriptions
    if isinstance(job, RecordHandler):
        return model_type_config.run_status_descriptions


def submit_docker_local(
    job: Job,
    *,
    output_dir: Union[str, Path] = None,
    in_data: Optional[Union[str, Path]] = None,
    ref_data: Optional[Dict[str, Union[str, Path]]] = None,
    model_path: Optional[Union[str, Path]] = None,
) -> ContainerRun:
    """Run a `Job` from a local docker container.

    While the Job is running, the `submit_docker_local` function will
    block and periodically send back status updates as the Job progresses.

    Note: Please ensure the Job has not already been submitted. If the
    Job has already been submitted, the run will fail.

    Args:
        job: The job to run. May be either a ``Model`` or ``RecordHandler``.
        output_dir: A directory path to write the output to. If the directory
            does not exist, the path will be created for you. If no path
            is specified, the current working directory is used.
        in_data: Input data path.
        ref_data: Reference data path or dict where values are reference data path
        model_path: If you are running a ``RecordHandler``, this is the path
            to the model that is being ran.

    Returns:
        A ``ContainerRun`` that can be used to manage the lifecycle
        of the associated local docker container.
    """
    if not output_dir:
        output_dir = os.getcwd()
    job.submit_manual()
    run = ContainerRun.from_job(job)
    run.configure_output_dir(str(output_dir))
    if job.instance_type == GPU:
        log.info("Configuring GPU for model training")
        try:
            run.configure_gpu()
            log.info("GPU device found!")
        except ContainerRunError:
            log.warn("Could not configure GPU. Continuing with CPU")

    # If our `job` instance already has data sources set and they are
    # local files, then we'll implicitly use them as the data sources
    # for this local job
    if in_data is None and Path(job.data_source).is_file():
        in_data = job.data_source
    if in_data:
        run.configure_input_data(in_data)

    ref_data_obj = ref_data_factory(ref_data)
    # If we did not receive any local ref data but the job itself
    # has ref data, we'll attempt to use the job's ref data instead
    if ref_data_obj.is_empty and not job.ref_data.is_empty:
        ref_data_obj = job.ref_data
    run.configure_ref_data(ref_data_obj)

    if model_path:
        run.configure_model(model_path)
    run.start()
    poll(job)
    run.extract_output_dir(str(output_dir))
    return run


def do_api_call(
    method: str,
    path: str,
    query_params: Optional[dict] = None,
    body: Optional[dict] = None,
    headers: Optional[dict] = None,
) -> dict:
    """
    Make a direct API call to Gretel Cloud.

    Args:
        method: "get", "post", etc
        path: The full path to make the request to, any path params must be already included.
            Example: "/users/me"
        query_params: Optional URL based query parameters
        body: An optional JSON payload to send
        headers: Any custom headers that need to bet set.

    NOTE:
        This function will automatically inject the appropiate API hostname and
        authentication from the Gretel configuration.
    """
    if headers is None:
        headers = {}

    method = method.upper()

    if not path.startswith("/"):
        path = "/" + path

    api = get_session_config()._get_api_client()

    # Utilize the ApiClient method to inject the proper authentication
    # into our headers, since Gretel only uses header-based auth we don't
    # need to pass any other data into this
    #
    # NOTE: This function does a pointer-like update of ``headers``
    api.update_params_for_auth(
        headers, None, api.configuration.auth_settings(), None, None, None
    )

    url = api.configuration.host + path

    response = api.request(
        method, url, query_params=query_params, body=body, headers=headers
    )

    resp_dict = json.loads(response.data.decode())
    return resp_dict.get("data")
