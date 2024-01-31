import json
import signal

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

    runner: Optional[str] = None

    data_source: Optional[str] = None
    ref_data: Optional[RefData] = None

    model_id: Optional[str] = None

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
        self._model = None

        self._project = None
        self._project_id = None
        if self.config.default_project_name:
            self._project_id = self.config.default_project_name

        self.cleanup_methods = []
        self._shutting_down = False
        signal.signal(signal.SIGINT, self._cleanup)

        self._print_copyright()

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
            click.UsageError("Invalid output format.", ctx=self.ctx)
        if not ok:
            self.exit(1)

    def set_project(self, project_name: str):
        self._project_id = project_name

    def set_model(self, model_id: str):
        self.model_id = model_id

    def set_record_handler(self, record_handler_id: str):
        self.record_handler_id = record_handler_id

    @property
    def model(self) -> Model:
        if self._model:
            return self._model
        self._model = self.project.get_model(self.model_id)
        return self._model

    @property
    def project(self) -> Project:
        if self._project:
            return self._project
        if not self._project_id:
            raise click.BadArgumentUsage("A project must be specified.")
        self._project = get_project(name=self._project_id, session=self.session)
        return self._project

    def ensure_project(self):
        if not self.project:
            raise click.UsageError(
                "A project must be specified since no default was found.", ctx=self.ctx
            )

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
        envvar="GRETEL_DEFAULT_PROJECT",
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


def get_model(ctx, param: click.Option, value: str):
    model_obj = ModelObjectReader(value)
    sc: SessionContext = ctx.ensure_object(SessionContext)
    if not sc.model_id:
        model_obj.apply(sc)
    return sc.model_id or value


def model_option(fn):
    return click.option(  # type:ignore
        "--model-id",
        metavar="UID",
        help="Specify the model.",
        callback=get_model,
        required=True,
    )(fn)


def record_handler_option(fn):
    def callback(ctx, param: click.Option, value: str):
        record_handler_obj = JobObjectReader(value)
        sc: SessionContext = ctx.ensure_object(SessionContext)
        record_handler_obj.apply(sc)
        return sc.record_handler_id or value

    return click.option(  # type:ignore
        "--record-handler-id",
        metavar="UID",
        help="Specify the record handler id.",
        callback=callback,
        required=True,
    )(fn)


def ref_data_option(fn):
    def callback(ctx, param: click.Option, value: Union[RefData, Tuple[str]]):
        gc: SessionContext = ctx.ensure_object(SessionContext)
        return value or gc.ref_data

    return click.option(
        "--ref-data",
        metavar="PATH",
        multiple=True,
        callback=callback,
        help="Specify additional model or record handler reference data.",
    )(fn)


def parse_file(val: str) -> dict:
    contents = val
    obj = {}
    try:
        if Path(val).is_file():
            contents = Path(val).read_text()
    except OSError:
        pass
    try:
        obj = json.loads(contents)
    except Exception:
        pass
    return obj


class ModelObjectReader:
    """Reads a model config and configures the ``SessionContext`` based
    on the contents of the model.
    """

    def __init__(self, input: str):
        self.input = input
        self.model = parse_file(input)

    def apply(self, sc: SessionContext):
        model_id = self.model.get("uid")
        project_id = self.model.get("project_id")
        runner = self.model.get("runner_mode")
        if project_id:
            sc.set_project(project_id)
        if model_id:
            sc.set_model(model_id)
        else:
            # if there isn't a model id, then we implicitly assume
            # the original input was a model id rather than a model
            # object.
            sc.set_model(self.input)
        if runner:
            sc.runner = (
                RunnerMode.CLOUD.value if runner == "cloud" else RunnerMode.LOCAL.value
            )


class JobObjectReader:
    """Reads a record handler config and configures the ``SessionContext`` based
    on the contents of the record handler."""

    def __init__(self, input: str):
        self.input = input
        self.record_handler = parse_file(input)

    def apply(self, sc: SessionContext):
        record_handler_id = self.record_handler.get("uid")
        model_id = self.record_handler.get("model_id")
        project_id = self.record_handler.get("project_id")
        runner = self.record_handler.get("runner_mode")
        if record_handler_id:
            sc.set_record_handler(record_handler_id)
        else:
            sc.set_record_handler(self.input)
        if project_id:
            sc.set_project(project_id)
        if model_id:
            sc.set_model(model_id)
        if runner:
            sc.runner = (
                RunnerMode.CLOUD.value if runner == "cloud" else RunnerMode.LOCAL.value
            )


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
