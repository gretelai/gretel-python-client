import functools
import json
import os
import re
import signal

from functools import cached_property
from pathlib import Path
from typing import Any, Callable, Optional, Tuple, Union

import click

from gretel_client.cli.utils.parser_utils import RefData
from gretel_client.config import (
    ClientConfig,
    configure_custom_logger,
    get_session_config,
    RunnerMode,
)
from gretel_client.models.config import get_status_description, StatusDescriptions
from gretel_client.projects.common import WAIT_UNTIL_DONE
from gretel_client.projects.docker import check_docker_env
from gretel_client.projects.exceptions import DockerEnvironmentError
from gretel_client.projects.jobs import Job, WaitTimeExceeded
from gretel_client.projects.models import Model
from gretel_client.projects.projects import get_project, Project
from gretel_client.rest.exceptions import ApiException

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
    """This class is used to print CLI progress and debug messages
    to the console.

    All messages are printed out to ``stderr``. All progress and debug messages
    go to ``stderr`` and any response type data goes to ``stdout``. This keeps
    ``stdout`` clean so that output may be piped and parsed by downstream
    commands.
    """

    def __init__(self, debug: bool = False):
        self.debug_mode = debug

    def _format_object(self, obj: Any) -> Optional[str]:
        if obj:
            return json.dumps(obj, indent=4)

    def info(
        self, msg: str = "", data: Optional[Any] = None, nl=True, prefix_nl=False
    ) -> None:
        """Prints general info statements to the console. Use this log
        level if you want to print messages that indicate progress or
        state change.

        Args:
            msg: The message to print.
        """
        if data:
            msg = f"{msg}\n{self._format_object(data)}"
        if prefix_nl:
            click.echo("", err=True)
        click.echo(
            click.style("INFO: ", fg="green") + msg,
            err=True,
            nl=nl,
        )

    def warn(self, msg: str) -> None:
        self.warning(msg)

    def warning(self, msg: str, prefix_nl=False) -> None:
        """Prints warn log messages."""
        if prefix_nl:
            click.echo("", err=True)
        click.echo(
            click.style("WARN: ", fg="yellow") + msg,
            err=True,
        )

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

    _project: Optional[Project] = None
    """The project to use for this command (explicitly specified)."""

    _model_ref: Optional[dict] = None
    """Dictionary with a model reference (UID + potentially other coordinates)."""

    _model: Optional[Model] = None
    """Model resolved from _model_ref, if specified."""

    runner: Optional[str] = None

    def __init__(
        self,
        ctx: click.Context,
        output_fmt: str,
        debug: bool = False,
        *,
        session: Optional[ClientConfig] = None,
    ):
        self.debug = debug
        self.verbosity = 0
        self.output_fmt = output_fmt
        self.config = session or get_session_config()
        self.log = Logger(self.debug)
        configure_custom_logger(self.log)
        self.ctx = ctx

        self.cleanup_methods = []
        self._shutting_down = False
        signal.signal(signal.SIGINT, self._cleanup)

        self._print_copyright()

    def validate(self):
        """
        This method is invoked after all options are parsed and their callbacks
        have been invoked, but before the actual command function begins.
        """
        # If a model reference was specified, resolve it now.
        if self._model_ref:
            self._model = self.project.get_model(self._model_ref["uid"])

    @property
    def session(self) -> ClientConfig:
        return self.config

    def exit(self, exit_code: int = 0):
        self.ctx.exit(exit_code)

    def _print_copyright(self):
        if self.ctx.invoked_subcommand == "configure":
            click.echo(
                click.style("\nGretel.ai COPYRIGHT Notice\n", fg="yellow"), err=True
            )
            click.echo(_copyright_data + "\n\n", err=True)

    def print(
        self, *, ok: bool = True, message: str = None, data: Union[list, dict, str]
    ):
        if self.output_fmt == "json":
            if isinstance(data, str):
                click.echo(data)
            else:
                click.echo(json.dumps(data, indent=4, default=str))
        else:
            raise click.UsageError("Invalid output format.", ctx=self.ctx)
        if not ok:
            self.exit(1)

    def set_project(self, project_name: str, *, source: str = "unknown"):
        project = get_project(name=project_name, session=self.session)
        if not self._project:
            self._project = project
            self._project_source = source
            self.log.debug(
                f"using project '{project.name}' ({project.project_guid}) from {source}"
            )
        elif self._project.project_guid != project.project_guid:
            raise click.Abort(
                f"Project '{project_name}' specified via {source} differs from project '{self._project.name}' specified via {self._project_source}"
            )

    def set_model_ref(self, model_ref: dict):
        if self._model_ref:
            raise click.BadOptionUsage(
                "--model-id", "Cannot specify multiple model IDs."
            )
        self._model_ref = model_ref

    @cached_property
    def model(self) -> Model:
        if not self._model:
            raise click.BadOptionUsage(
                "--model-id", "No model was specified on the command line."
            )
        return self._model

    @cached_property
    def project(self) -> Project:
        if self._project:
            return self._project

        if project_from_env := os.getenv("GRETEL_DEFAULT_PROJECT"):
            self.set_project(
                project_from_env, source="GRETEL_DEFAULT_PROJECT environment variable"
            )
        elif session_default_project := self.session.default_project_name:
            self.set_project(session_default_project, source="session configuration")
        else:
            raise click.BadArgumentUsage("A project must be specified.")

        return self._project

    def _cleanup(self, sig, frame):
        if self._shutting_down:
            self.log.warn("Got a second interrupt. Shutting down.")
            self.exit(1)
        else:
            self._shutting_down = True
        self.log.warn("Got interrupt signal.")

        if self.cleanup_methods:
            self.log.warn("Attempting graceful shutdown.")
            for method in self.cleanup_methods:
                try:
                    method()
                except Exception as ex:
                    self.log.debug("Cleanup hook failed to run.", ex=ex)
        self.log.info("Quitting.")
        self.exit(0)

    def register_cleanup(self, method: Callable):
        self.cleanup_methods.append(method)


_pass_session = click.make_pass_decorator(SessionContext, ensure=True)


def pass_session(fn):
    @_pass_session
    @functools.wraps(fn)
    def wrapped(sc: SessionContext, *args, **kwargs):
        sc.validate()
        fn(sc, *args, **kwargs)

    return wrapped


def project_option(fn):
    def callback(ctx, param: click.Option, value: str):
        sc: SessionContext = ctx.ensure_object(SessionContext)
        if value is not None:
            sc.set_project(value, source="--project flag")
        return value

    return click.option(  # type:ignore
        "--project",
        help="Gretel project to execute command from.",
        metavar="NAME",
        callback=callback,
    )(fn)


def runner_option(fn):
    def callback(ctx, param: click.Option, value: str):
        sc: SessionContext = ctx.ensure_object(SessionContext)
        selected_runner = sc.runner or value
        if selected_runner == RunnerMode.LOCAL.value:
            try:
                check_docker_env()
            except DockerEnvironmentError as ex:
                raise DockerEnvironmentError(
                    "Runner is local, but docker is not running. Please check that docker is installed and running."
                ) from ex
        return selected_runner

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
    def callback(ctx, param: click.Option, value: Optional[dict]):
        if value is None:
            return None
        sc: SessionContext = ctx.ensure_object(SessionContext)
        if project_id := value.get("project_id"):
            sc.set_project(project_id, source="--model-id file")
        if runner := value.get("runner_mode"):
            sc.runner = (
                RunnerMode.CLOUD.value if runner == "cloud" else RunnerMode.LOCAL.value
            )
        sc.set_model_ref(value)
        return value

    return click.option(  # type:ignore
        "--model-id",
        metavar="UID",
        help="Specify the model.",
        type=JobRefDictParamType("model"),
        callback=callback,
        required=True,
    )(fn)


def record_handler_option(fn):
    def callback(ctx, param: click.Option, value: Optional[dict]):
        if value is None:
            return None
        sc: SessionContext = ctx.ensure_object(SessionContext)
        if project_id := value.get("project_id"):
            sc.set_project(project_id, source="--record-handler-id file")
        # The runner mode isn't interesting here, as if we already specify
        # a record handler via the command line, we're not running anything
        # new.
        return value

    return click.option(  # type:ignore
        "--record-handler-id",
        metavar="UID",
        help="Specify the record handler id.",
        type=JobRefDictParamType("record handler"),
        callback=callback,
        required=True,
    )(fn)


def ref_data_option(fn):
    return click.option(
        "--ref-data",
        metavar="PATH",
        multiple=True,
        help="Specify additional model or record handler reference data.",
    )(fn)


class JobRefDictParamType(click.ParamType):
    """
    Command-line parameter that can either be a job UID or a JSON file
    produced by the CLI, containing at minimum a "uid" field, and possibly
    also other fields that may be taken into account, such as "project_id"
    and "runner_mode".

    The parsed value is always a dict, even if a single UID is given (in that
    case, the dict will contain a single "uid" entry).
    """

    _UID_REGEX = re.compile(r"^[a-fA-F0-9]{24}$")

    def __init__(self, name):
        self.name = name

    def convert(self, value, param, ctx):
        if isinstance(value, dict):
            return value

        if isinstance(value, str):
            value = value.strip()
            if JobRefDictParamType._UID_REGEX.match(value):
                # If this looks like a job UID, we prioritize this
                # interpretation (in the unlikely event that this shadows
                # a file name, this can be fixed by prefixing the file name
                # with ``./``).
                return {"uid": value}

        # Attempt to parse the parameter as a JSON file path
        try:
            if Path(value).is_file():
                with open(value, "r") as f:
                    obj = json.load(f)

                if not isinstance(obj, dict):
                    self.fail(f"file {value} does not contain a JSON object")
                if not obj.get("uid"):
                    self.fail(
                        f"JSON object in file {value} does not contain a 'uid' field"
                    )
                return obj
        except Exception as ex:
            self.fail(f"error reading file '{value}': {str(ex)}")


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


class _KVPairsType(click.ParamType):
    name = "key/value pairs"

    def convert(self, value, param, ctx):
        if isinstance(value, dict):
            return value

        try:
            value = value.strip()
            if not value:
                return {}
            return {
                k.strip(): v.strip()
                for kvpair in value.split(",")
                for k, v in (kvpair.split("="),)
            }
        except ValueError:
            self.fail(f"{value!r} is not a valid key/value pair list")


KVPairs = _KVPairsType()
