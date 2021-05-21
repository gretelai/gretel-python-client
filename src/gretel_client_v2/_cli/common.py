import sys
import json
import traceback
from typing import Union

import click

from gretel_client_v2.config import get_session_config
from gretel_client_v2.projects import get_project
from gretel_client_v2.rest.exceptions import NotFoundException, UnauthorizedException


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

    def error(self, msg: str = None, ex: Exception = None, include_tb: bool = True):
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
            click.echo(ex, err=True)
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb)

    def debug(self, msg: str):
        """Prints a debug message to the console if ``debug_mode`` is
        enabled.

        Args:
            msg: The message to print.
        """
        if self.debug_mode:
            click.echo(click.style("DEBUG: ", fg="yellow") + msg, err=True)


class SessionContext(object):
    def __init__(self, ctx: click.Context, output_fmt: str, debug: bool = False):
        self.debug = debug
        self.verbosity = 0
        self.output_fmt = output_fmt
        self.config = get_session_config()
        self.log = Logger(self.debug)
        self.ctx = ctx

        if self.config.default_project_name:
            self.set_project(self.config.default_project_name)
        else:
            self.project = None

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

    def ensure_project(self):
        if not self.project:
            raise click.UsageError(
                "A project must be specified since no default was found.", ctx=self.ctx
            )


pass_session = click.make_pass_decorator(SessionContext, ensure=True)


def project_option(fn):
    def callback(ctx, param: click.Option, value: str):
        gc: SessionContext = ctx.ensure_object(SessionContext)
        if value is not None:
            gc.set_project(value)
        gc.ensure_project()
        return value

    return click.option(
        "--project",
        allow_from_autoenv=True,
        help="Gretel project to execute command from",
        metavar="NAME",
        callback=callback,
    )(fn)
