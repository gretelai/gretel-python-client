import os
from pathlib import Path
import sys
from typing import Optional, Union

import click

from gretel_client.cli.common import SessionContext, get_description_set, poll_and_print
from gretel_client.projects.common import WAIT_UNTIL_DONE
from gretel_client.projects.docker import ContainerRun
from gretel_client.projects.jobs import Job


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


def submit_docker_local(
    job: Job,
    *,
    output_dir: Union[str, Path] = None,
    in_data: Optional[Union[str, Path]] = None,
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
    if in_data:
        run.configure_input_data(in_data)
    if not in_data and job.data_source:
        run.configure_input_data(job.data_source)
    if model_path:
        run.configure_model(model_path)
    run.start()
    poll(job)
    run.extract_output_dir(str(output_dir))
    return run
