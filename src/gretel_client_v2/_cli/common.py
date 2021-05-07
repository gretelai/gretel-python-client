import json
from typing import Union

import click

from gretel_client_v2.config import get_session_config
from gretel_client_v2.projects import get_project
from gretel_client_v2.rest.exceptions import NotFoundException, UnauthorizedException


def validate_project(project_name: str) -> bool:
    try:
        get_project(name=project_name)
    except (UnauthorizedException, NotFoundException):
        return False
    return True


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
        click.echo(f"INFO: {msg}", err=True)

    def error(self, msg):
        click.echo(f"ERROR: {msg}", err=True)

    def debug(self, msg):
        if self.debug_mode:
            click.echo(f"DEBUG: {msg}", err=True)


class SessionContext(object):
    def __init__(self, ctx: click.Context, output_fmt: str, debug: bool = False):
        self.debug = debug
        self.verbosity = 0
        self.output_fmt = output_fmt
        self.config = get_session_config()
        self.project = self.config.default_project_name
        self.log = Logger(self.debug)
        self.ctx = ctx

    def exit(self, exit_code: int = 0):
        self.ctx.exit(exit_code)

    def print(self, *, ok: bool = True, message: str = None, data: Union[list, dict]):
        if self.output_fmt == "json":
            click.echo(json.dumps(data, indent=4))
        else:
            click.UsageError("Invalid output format", ctx=self.ctx)
        if not ok:
            self.exit(1)


pass_session = click.make_pass_decorator(SessionContext, ensure=True)


def project_option(fn):
    def callback(ctx, param: str, value: str):
        gc = ctx.ensure_object(SessionContext)
        gc.project = value
        return value
    return click.option(
        "--project",
        allow_from_autoenv=True,
        help="Gretel project to execute command from",
    )
