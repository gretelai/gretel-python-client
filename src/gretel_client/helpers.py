import sys

import click

from gretel_client.cli.common import SessionContext, get_description_set, poll_and_print
from gretel_client.projects.common import WAIT_UNTIL_DONE
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
    descriptions = get_description_set(job)
    if not descriptions:
        raise ValueError("Cannot fetch Job polling descriptions")
    sc.print(data=job.print_obj)
    poll_and_print(job, sc, job.runner_mode, descriptions, wait)
