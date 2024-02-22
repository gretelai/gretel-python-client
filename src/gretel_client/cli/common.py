from __future__ import annotations

import functools
import json
import os
import re
import signal

from functools import cached_property
from pathlib import Path
from typing import Any, Callable, Optional, Union

import click

from gretel_client.config import (
    add_session_context,
    ClientConfig,
    configure_custom_logger,
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

CLI_SESSION_METADATA = {"cli": "1"}


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


def _json_default_handler(obj: Any) -> Any:
    # Figure out if the object is a MagicMock. If it is, calling to_dict()
    # would result in an infinite recursion.
    # We need to check the __module__ and __qualname__ properties of the type,
    # as we don't want to create an import dependency.
    try:
        if (type(obj).__module__, type(obj).__qualname__) == (
            "unittest.mock",
            "MagicMock",
        ):
            return str(obj)
    except:
        pass

    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    return str(obj)


def _model_runner_modes(
    model: Model,
) -> list[str]:
    """
    Returns a list of compatible runner modes for a given model.

    If there are multiple compatible runner modes, the first element
    in the list should be the preferred runner mode for this model.

    Args:
        model: the model.

    Returns:
        the list of compatible runner modes for the given model.
    """
    if model.runner_mode == RunnerMode.HYBRID:
        return [RunnerMode.HYBRID]
    if model.is_cloud_model:
        return [RunnerMode.LOCAL, RunnerMode.CLOUD, RunnerMode.MANUAL]
    return [RunnerMode.LOCAL, RunnerMode.MANUAL]


def _select_runner_mode(
    runner_arg: Optional[str],
    session_runner_mode: str,
    project: Project,
    model: Optional[Model],
    model_json: Optional[dict],
) -> tuple[str, str, bool]:
    """
    Selects the effective runner mode from various sources.

    The runner mode selection follows the following rules (in order of priority):
    - An explicit --runner argument will always be respected.
    - The runner mode found in a --model-id JSON file will be used ("manual" being mapped to "local")
    - The project-wide setting will be respected.
    - The session default will be used whenever it is compatible with the model configuration (or when there is no model)
    - Otherwise, the preferred runner mode for the model will be used.

    The return value indicates how the runner mode was determined, and whether it
    came from an explicit setting (flag).

    Args:
        runner_arg: the explicit --runner argument.
        session_runner_mode: default runner mode configured for the session.
        project: the project context.
        model: the model from the context (if any).
        model_json: the parsed JSON file specified as the model reference (if any).

    Returns:
        tuple[str, str, bool], where the first element is the chosen runner mode,
            the second identifies the source from which this runner mode was
            determined, and the third indicates if the choice was due to an explicitly
            provided flag.
    """

    if runner_arg:
        return runner_arg, "--runner flag", True

    if model_json_runner_mode := (model_json or {}).get("runner_mode"):
        runner_mode = (
            RunnerMode.LOCAL
            if model_json_runner_mode == RunnerMode.MANUAL
            else model_json_runner_mode
        )
        return runner_mode, "--model-id JSON file", True

    if project.runner_mode:
        return project.runner_mode, "project setting", False

    if not model:
        return session_runner_mode, "session default", False

    allowed_runner_modes = _model_runner_modes(model)
    if session_runner_mode in allowed_runner_modes:
        return session_runner_mode, "session default", False

    return allowed_runner_modes[0], f"model configuration", False


def _determine_runner_mode(
    sc: SessionContext,
    runner_arg: str,
    project: Project,
    model: Optional[Model],
    model_json: Optional[dict],
):
    runner_mode, runner_mode_source, explicit = _select_runner_mode(
        runner_arg,
        sc.session.default_runner,
        project,
        model,
        model_json,
    )

    if project.runner_mode and runner_mode != project.runner_mode:
        raise click.Abort(
            f"Runner mode '{runner_mode}' from {runner_mode_source} is incompatible with project setting '{project.runner_mode}'."
        )

    if model:
        allowed_runner_modes = _model_runner_modes(model)
        if runner_mode not in allowed_runner_modes:
            raise click.Abort(
                f"Runner mode '{runner_mode}' from {runner_mode_source} is not valid for model {model.id}, model only allows the following runner modes: {allowed_runner_modes}"
            )

    # If the runner mode was chosen explicitly (or matches the configured session
    # default), all is well. Otherwise, we need to inform the user, and in some
    # cases require explicit consent for deviating from the session default.
    if not explicit and runner_mode != sc.session.default_runner:
        if runner_mode == RunnerMode.CLOUD:
            # Do not switch to cloud implicitly, require explicit consent.
            raise click.Abort(
                f"{runner_mode_source} prescribes 'cloud' runner mode, but your configured default "
                f"runner mode is '{sc.session.default_runner}'. Please specify '--runner cloud' "
                "explicitly to confirm using cloud runner mode (note that this will upload any "
                "input data to Gretel Cloud)."
            )

        # For all other runner modes, switch the runner mode implicitly, but inform the
        # user of the switch.
        sc.log.info(
            (
                f"{runner_mode_source} requires runner mode '{runner_mode}', "
                f"but your configured default runner mode is '{sc.session.default_runner}'."
            )
        )
        sc.log.info(f"Using '{runner_mode}' to conform to project configuration.")

    return runner_mode


class SessionContext(object):

    _project: Optional[Project] = None
    """The project to use for this command (explicitly specified)."""

    _model_ref: Optional[dict] = None
    """Dictionary with a model reference (UID + potentially other coordinates)."""

    _model: Optional[Model] = None
    """Model resolved from _model_ref, if specified."""

    _runner: Optional[str] = None

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
        self.config = add_session_context(
            session=session, client_metrics=CLI_SESSION_METADATA
        )
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

        # If this command takes a --runner argument, validate the runner eagerly.
        if "runner" in self.ctx.params or self._runner:
            # Runner mode is determined as follows:
            # 1. Explicitly specified runner mode or project runner mode
            #    (if both are specified, they must match).
            # 2. Model runner mode (if the command runs in the context of a model,
            #    i.e., `gretel models run`).
            # 3. Configured session runner mode.
            self._runner = _determine_runner_mode(
                self,
                self._runner,
                self.project,
                self.model if self._model_ref else None,
                self._model_ref,
            )

            if self._runner == RunnerMode.LOCAL:
                try:
                    check_docker_env()
                except DockerEnvironmentError as ex:
                    raise DockerEnvironmentError(
                        "Runner is local, but docker is not running. Please check that docker is installed and running."
                    ) from ex

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
        self,
        *,
        ok: bool = True,
        message: str = None,
        data: Union[list, dict, str],
        auto_printer: Optional[Callable[[Any], None]] = None,
    ):
        output_fmt = self.output_fmt
        if output_fmt == "auto" and not auto_printer:
            output_fmt = "json"

        if output_fmt == "auto":
            auto_printer(data)
        elif output_fmt == "json":
            if isinstance(data, str):
                click.echo(data)
            else:
                click.echo(json.dumps(data, indent=4, default=_json_default_handler))
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

    def set_runner(self, runner: str):
        if self._runner and self._runner != runner:
            raise click.BadOptionUsage(
                "--runner",
                f"specified conflicting values '{runner}' and '{self._runner}'",
            )
        self._runner = runner

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

    @cached_property
    def runner(self) -> str:
        if not self._runner:
            raise click.BadOptionUsage("--runner", "no runner mode was specified")
        return self._runner

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


def pass_session(fn):
    # We're not using the make_pass_decorator, because the context of the
    # session context needs to be adjusted manually (because `ensure_object`
    # is called from flag callbacks, it may be initially constructed with
    # the parent context, rather than the context of the leaf command).
    @click.pass_context
    def wrapper(ctx: click.Context, *args, **kwargs):
        sc = ctx.ensure_object(SessionContext)
        sc.ctx = ctx
        sc.validate()
        fn(sc, *args, **kwargs)

    return functools.update_wrapper(wrapper, fn)


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
    def callback(ctx, param: click.Option, value: Optional[str]):
        if value is None:
            return None
        sc: SessionContext = ctx.ensure_object(SessionContext)
        sc.set_runner(value)
        return value

    return click.option(  # type: ignore
        "--runner",
        metavar="TYPE",
        type=click.Choice([m.value for m in RunnerMode], case_sensitive=False),
        callback=callback,
        show_default="project setting, or setting from ~/.gretel/config.json",
        help="Determines where to schedule the model run.",
    )(fn)


def model_option(fn):
    def callback(ctx, param: click.Option, value: Optional[dict]):
        if value is None:
            return None
        sc: SessionContext = ctx.ensure_object(SessionContext)
        if project_id := value.get("project_id"):
            sc.set_project(project_id, source="--model-id file")
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
