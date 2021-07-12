import json
import signal
import sys
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union
from urllib.parse import urlparse

import click
import requests

from gretel_client.config import (
    RunnerMode,
    configure_custom_logger,
    get_session_config,
)
from gretel_client.projects import get_project
from gretel_client.projects.common import ModelType, WAIT_UNTIL_DONE
from gretel_client.projects.jobs import Job, WaitTimeExceeded
from gretel_client.projects.models import Model
from gretel_client.projects.projects import Project
from gretel_client.projects.records import RecordHandler
from gretel_client.rest.exceptions import ApiException, NotFoundException


ExT = Union[str, Exception]


_copyright_data = """
The Gretel CLI and Python SDK, installed through the "gretel-client"
package or other mechanism is free and open source software under
the Apache 2.0 License.

When using the CLI or SDK, you may launch "Gretel Worker(s)"
that are hosted in your local environment as containers. These
workers are launched automatically when running commands that create
models or process data records.

The "Gretel Worker" and all code within it is copyrighted and an
extension of the Gretel Service and licensed under the Gretel.ai
Terms of Service.  These terms can be found at https://gretel.ai/terms
section G paragraph 2.
"""


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

    def _format_object(self, obj: Any) -> Optional[str]:
        if obj:
            return json.dumps(obj, indent=4)

    def info(self, msg: str = "", data: Optional[Any] = None):
        """Prints general info statements to the console. Use this log
        level if you want to print messages that indicate progress or
        state change.

        Args:
            msg: The message to print.
        """
        if data:
            msg = f"{msg}\n{self._format_object(data)}"
        click.echo(click.style("INFO: ", fg="green") + msg, err=True)

    def warn(self, msg):
        """Print warn log messages"""
        click.echo(click.style("WARN: ", fg="yellow") + msg, err=True)

    def error(self, msg: str = None, ex: ExT = None, include_tb: bool = True):
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
        if ex:
            self.hint(ex)

    def hint(self, ex: ExT):
        hint = None
        try:
            hint = get_hint_for_error(ex)
        except Exception as ex:
            self.debug("Could not get hint", ex=ex)
        if hint:
            click.echo(click.style("HINT: ", fg="blue") + hint, err=True)

    def debug(self, msg: str, ex: ExT = None):
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


def _naming_hint(ex: ExT) -> Optional[str]:
    if isinstance(ex, ApiException):
        resp = json.loads(ex.body)
        ctx = resp.get("context")
        matched = False
        obj = ""
        if ctx:
            if isinstance(ctx, dict):
                matched = "name" in ctx
                obj = "Project"
            if isinstance(ctx, list):
                for err in ctx:
                    if "name" in err.get("loc", []):
                        matched = True
                        obj = "Model"

        if matched:
            return (
                f"{obj} names must be DNS compliant. "
                "No upper-case letters, underscores or periods. "
                "Names may not end in a dash and must be between 3 and 63 characters."
            )


hints = [_naming_hint]


def get_hint_for_error(ex: ExT) -> Optional[str]:
    for hint in hints:
        try:
            hint_msg = hint(ex)
            if hint_msg:
                return hint_msg
        except Exception:
            pass


class SessionContext(object):

    runner: Optional[str] = None

    data_source: Optional[str] = None

    def __init__(self, ctx: click.Context, output_fmt: str, debug: bool = False):
        self.debug = debug
        self.verbosity = 0
        self.output_fmt = output_fmt
        self.config = get_session_config()
        self.log = Logger(self.debug)
        configure_custom_logger(self.log)
        self.ctx = ctx
        self._model = None

        self._project = None
        self._project_id = None
        if self.config.default_project_name:
            self._project_id = self.config.default_project_name

        self.cleanup_methods = []
        self._shutting_down = False
        signal.signal(signal.SIGINT, self._cleanup)

        self._print_copyright()

    def exit(self, exit_code: int = 0):
        self.ctx.exit(exit_code)

    def _print_copyright(self):
        if self.ctx.invoked_subcommand == "configure":
            click.echo(
                click.style("\nGretel.ai COPYRIGHT Notice\n", fg="yellow"), err=True
            )
            click.echo(_copyright_data + "\n\n", err=True)

    def print(self, *, ok: bool = True, message: str = None, data: Union[list, dict]):
        if self.output_fmt == "json":
            click.echo(json.dumps(data, indent=4))
        else:
            click.UsageError("Invalid output format", ctx=self.ctx)
        if not ok:
            self.exit(1)

    def set_project(self, project_name: str):
        self._project_id = project_name

    def set_model(self, model_id: str):
        self.model_id = model_id

    @property
    def model(self) -> Model:
        if self._model:
            return self._model
        else:
            try:
                self._model = self.project.get_model(self.model_id)
                return self._model
            except NotFoundException as ex:
                self.log.debug(f"Could not get model {self.model_id}", ex=ex)
                raise click.BadParameter(
                    (
                        f"The model `{self.model_id}` was not found in the "
                        f"project `{self.project.name}`. "
                    )
                )

    @property
    def project(self) -> Project:
        if self._project:
            return self._project
        if not self._project_id:
            raise click.BadArgumentUsage("A project must be specified.")
        else:
            try:
                self._project = get_project(name=self._project_id)
                return self._project
            except Exception as ex:
                self.log.error(ex=ex, include_tb=True)
                raise click.BadArgumentUsage(
                    f"Could not load the specified project `{self._project_id}`"
                ) from ex

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
        sc: SessionContext = ctx.ensure_object(SessionContext)
        if value is not None:
            sc.set_project(value)
        sc.ensure_project()
        return sc._project_id or value

    return click.option(  # type:ignore
        "--project",
        allow_from_autoenv=True,
        help="Gretel project to execute command from",
        metavar="NAME",
        callback=callback,
    )(fn)


def runner_option(fn):
    def callback(ctx, param: click.Option, value: str):
        sc: SessionContext = ctx.ensure_object(SessionContext)
        return sc.runner or value

    return click.option(  # type: ignore
        "--runner",
        metavar="TYPE",
        type=click.Choice([m.value for m in RunnerMode], case_sensitive=False),
        default=lambda: get_session_config().default_runner,
        callback=callback,
        show_default="from ~/.gretel/config.json",
        help="Determines where to schedule the model run.",
    )(fn)


def model_option(fn):
    def callback(ctx, param: click.Option, value: str):
        model_obj = ModelObjectReader(value)
        sc: SessionContext = ctx.ensure_object(SessionContext)
        model_obj.apply(sc)
        return sc.model_id or value

    return click.option(  # type:ignore
        "--model-id",
        metavar="UID",
        help="Specify the model.",
        callback=callback,
        required=True,
    )(fn)


class ModelObjectReader:
    """Reads a model config and configures the ``SessionContext`` based
    on the contents of the model.
    """

    def __init__(self, input: str):
        self.input = input
        self.model = self._maybe_parse_model(input)

    def _maybe_parse_model(self, val: str) -> dict:
        contents = val
        model_obj = {}
        try:
            if Path(val).is_file():
                contents = Path(val).read_text()
        except OSError:
            pass
        try:
            model_obj = json.loads(contents)
        except Exception:
            pass
        return model_obj

    def apply(self, sc: SessionContext):
        model_id = self.model.get("uid")
        project_id = self.model.get("project_id")
        runner = self.model.get("runner_mode")
        if project_id:
            sc.set_project(project_id)
        if model_id:
            sc.set_model(model_id)
            sc.data_source = sc.model.data_source
        if not model_id:
            # if there isn't a model id, then we implicitly assume
            # the original input was a model id rather than a model
            # object.
            sc.set_model(self.input)
        if runner:
            sc.runner = (
                RunnerMode.CLOUD.value if runner == "cloud" else RunnerMode.LOCAL.value
            )


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

record_classify_status_descriptions: StatusDescriptions = {
    "created": {
        "default": "A Record classify job has been queued.",
    },
    "pending": {
        "default": "A worker is being allocated to begin running a classification pipeline.",
        "cloud": "A Gretel Cloud worker is being allocated to begin classifying records.",
        "local": "A local container is being started and will begin classifying records.",
    },
    "active": {
        "default": "A worker has started!",
    },
}


def get_description_set(job: Job) -> Optional[dict]:
    if isinstance(job, Model):
        return model_create_status_descriptions
    if isinstance(job, RecordHandler):
        if job.model_type == ModelType.SYNTHETICS:
            return record_generation_status_descriptions
        if job.model_type == ModelType.TRANSFORMS:
            return record_transform_status_descriptions
        if job.model_type == ModelType.CLASSIFY:
            return record_classify_status_descriptions


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
            sc.log.info(
                "Option --wait=0 was specified, not waiting for the job completion."
            )
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
