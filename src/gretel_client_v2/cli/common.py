import json
import signal
import sys
import traceback
from pathlib import Path
from typing import Callable, Dict, Optional, Union
from urllib.parse import urlparse

import click
import requests

from gretel_client_v2.config import (
    RunnerMode,
    configure_custom_logger,
    get_session_config,
)
from gretel_client_v2.projects import get_project
from gretel_client_v2.projects.common import WAIT_UNTIL_DONE
from gretel_client_v2.projects.jobs import Job, WaitTimeExceeded
from gretel_client_v2.projects.models import Model


class Logger:
    """This classed is used to print CLI progress a debug messages
    to the console.

    You will note that all messages are printed out to ``stderr``. This is by
    design. All progress and debug messages go to ``stderr`` and any ``Response``
    type classes go to ``stdout``. This keeps ``stdout`` clean so that output
    can be piped and parsed by downstream commands.
    """

    def __init__(self, debug: bool = False):
        self.debug_mode = debug

    def info(self, msg):
        """Prints general info statements to the console. Use this log
        level if you want to print messages that indicate progress or
        state change.

        Args:
            msg: The message to print.
        """
        click.echo(click.style("INFO: ", fg="green") + msg, err=True)

    def warn(self, msg):
        """Print warn log messages"""
        click.echo(click.style("WARN: ", fg="yellow") + msg, err=True)

    def error(
        self, msg: str = None, ex: Union[str, Exception] = None, include_tb: bool = True
    ):
        """Logs an error to the terminal.

        Args:
            msg: The message to log.
            ex: The exception that triggered the log message.
            include_tb: If set to ``True`` the exception's traceback will be
                printed to the console if debug_mode is enabled.
        """
        if msg:
            click.echo(click.style("ERROR: ", fg="red") + msg, err=True)
        if include_tb and self.debug_mode:
            if ex:
                click.echo(ex, err=True)
            if include_tb:
                _, _, tb = sys.exc_info()
                traceback.print_tb(tb)

    def debug(self, msg: str, ex: Exception = None):
        """Prints a debug message to the console if ``debug_mode`` is
        enabled.

        Args:
            msg: The message to print.
        """
        if self.debug_mode:
            click.echo(click.style("DEBUG: ", fg="blue") + msg, err=True)
            if ex:
                _, _, tb = sys.exc_info()
                traceback.print_tb(tb)


class SessionContext(object):

    model: Optional[Model]

    def __init__(self, ctx: click.Context, output_fmt: str, debug: bool = False):
        self.debug = debug
        self.verbosity = 0
        self.output_fmt = output_fmt
        self.config = get_session_config()
        self.log = Logger(self.debug)
        configure_custom_logger(self.log)
        self.ctx = ctx

        if self.config.default_project_name:
            self.set_project(self.config.default_project_name)
        else:
            self.project = None

        self.cleanup_methods = []
        self._shutting_down = False
        signal.signal(signal.SIGINT, self._cleanup)

    def exit(self, exit_code: int = 0):
        self.ctx.exit(exit_code)

    def print(self, *, ok: bool = True, message: str = None, data: Union[list, dict]):
        if self.output_fmt == "json":
            click.echo(json.dumps(data, indent=4))
        else:
            click.UsageError("Invalid output format", ctx=self.ctx)
        if not ok:
            self.exit(1)

    def set_project(self, project_name: str):
        try:
            self.project = get_project(name=project_name)
        except Exception as ex:
            self.log.error(ex=ex, include_tb=True)
            raise click.BadArgumentUsage(
                f'The specified project "{project_name}" is not valid'
            ) from ex

    def set_model(self, model_id: str):
        if not self.project:
            raise click.BadArgumentUsage("Cannot set model. No project is set.")
        try:
            self.model = self.project.get_model(model_id)
        except Exception as ex:
            self.log.debug(
                f"Could not set model {model_id} {ex}"
            )  # todo(dn): better traceback log
            raise click.BadParameter("Invalid model.")

    def ensure_project(self):
        if not self.project:
            raise click.UsageError(
                "A project must be specified since no default was found.", ctx=self.ctx
            )

    def _cleanup(self, sig, frame):
        if self._shutting_down:
            self.log.warn("Got a second interrupt. Shutting down")
            self.exit(1)
        else:
            self._shutting_down = True
        self.log.warn("Got interupt signal.")

        if self.cleanup_methods:
            self.log.warn("Attemping graceful shutdown")
            for method in self.cleanup_methods:
                try:
                    method()
                except Exception as ex:
                    self.log.debug("Cleanup hook failed to run", ex=ex)
        self.log.info("Quitting")
        self.exit(0)

    def register_cleanup(self, method: Callable):
        self.cleanup_methods.append(method)


pass_session = click.make_pass_decorator(SessionContext, ensure=True)


def project_option(fn):
    def callback(ctx, param: click.Option, value: str):
        gc: SessionContext = ctx.ensure_object(SessionContext)
        if value is not None:
            gc.set_project(value)
        gc.ensure_project()
        return value

    return click.option(  # type:ignore
        "--project",
        allow_from_autoenv=True,
        help="Gretel project to execute command from",
        metavar="NAME",
        callback=callback,
    )(fn)


def runner_option(fn):
    return click.option(
        "--runner",
        metavar="TYPE",
        type=click.Choice([m.value for m in RunnerMode], case_sensitive=False),
        default=lambda: get_session_config().default_runner,
        show_default="from ~/.gretel/config.json",
        help="Determines where to schedule the model run.",
    )(fn)


def model_option(fn):
    def callback(ctx, param: click.Option, value: str):
        gc: SessionContext = ctx.ensure_object(SessionContext)
        gc.set_model(value)

    return click.option(  # type:ignore
        "--model-id", metavar="UID", help="Specify the model.", callback=callback
    )(fn)


StatusDescriptions = Dict[str, Dict[str, str]]

model_create_status_descriptions: StatusDescriptions = {
    "created": {
        "default": "Model creation has been queued.",
    },
    "pending": {
        "default": "A worker is being allocated to begin model creation.",
        "cloud": "A Gretel Cloud worker is being allocated to begin model creation.",
        "local": "A local container is being started to begin model creation.",
    },
    "active": {
        "default": "A worker has started creating your model!",
    },
}

record_generation_status_descriptions: StatusDescriptions = {
    "created": {
        "default": "A Record generation job has been queued.",
    },
    "pending": {
        "default": "A worker is being allocated to begin generating synthetic records.",
        "cloud": "A Gretel Cloud worker is being allocated to begin generating synthetic records.",
        "local": "A local container is being started to begin record generation.",
    },
    "active": {
        "default": "A worker has started!",
    },
}

record_transform_status_descriptions: StatusDescriptions = {
    "created": {
        "default": "A Record transform job has been queued.",
    },
    "pending": {
        "default": "A worker is being allocated to begin running a transform pipeline.",
        "cloud": "A Gretel Cloud worker is being allocated to begin transforming records.",
        "local": "A local container is being started to and will begin transforming records.",
    },
    "active": {
        "default": "A worker has started!",
    },
}


def get_status_description(
    descriptions: StatusDescriptions, status: str, runner: str
) -> str:
    status_desc = descriptions.get(status)
    if not status_desc:
        return ""
    return status_desc.get(runner, status_desc.get("default", ""))


def poll_and_print(
    job: Job,
    sc: SessionContext,
    runner: str,
    status_strings: StatusDescriptions,
    wait: int = WAIT_UNTIL_DONE,
    callback: Callable = None,
):
    try:

        for update in job.poll_logs_status(wait=wait, callback=callback):
            if update.transitioned:
                sc.log.info(
                    (
                        f"Status is {update.status}. "
                        f"{get_status_description(status_strings, update.status, runner)}"
                    )
                )
            for log in update.logs:
                msg = f"{log['ts']}  {log['msg']}"
                if log["ctx"]:
                    msg += f"\n{json.dumps(log['ctx'], indent=4)}"
                click.echo(msg, err=True)
            if update.error:
                sc.log.error(f"\t{update.error}")

    except WaitTimeExceeded:
        if wait == 0:
            sc.log.info("Option --wait=0 was specified, not waiting for the job completion.")
        else:
            sc.log.warn(
                f"Job hasn't completed after waiting for {wait} seconds. "
                f"Exiting the script, but the job will remain running until it reaches the end state."
            )
        sc.exit(0)


def download_artifacts(sc: SessionContext, output: str, job: Job):
    output_path = Path(output)
    output_path.mkdir(exist_ok=True, parents=True)
    sc.log.info(f"Downloading model artifacts to {output_path.resolve()}")
    for artifact_type, download_link in job.get_artifacts():
        try:
            art = requests.get(download_link)
            if art.status_code == 200:
                art_output_path = output_path / Path(urlparse(download_link).path).name
                with open(art_output_path, "wb+") as out:
                    sc.log.info(f"\tWriting {artifact_type} to {art_output_path}")
                    out.write(art.content)
        except requests.exceptions.HTTPError as ex:
            sc.log.error(
                f"\tCould not download {artifact_type}. You might retry this request.",
                ex=ex,
            )
