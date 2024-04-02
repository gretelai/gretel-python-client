import json
import traceback

from abc import ABC, abstractmethod
from typing import Dict, Generic, Optional, Type, TypeVar

import click
import urllib3.exceptions
import yaml

from gretel_client.cli.common import SessionContext
from gretel_client.projects.exceptions import GretelResourceNotFound
from gretel_client.rest.exceptions import ApiException
from gretel_client.rest_v1.exceptions import ApiException as ApiExceptionV1

E = TypeVar("E", bound="Exception")


class _ErrorHandler(ABC, Generic[E]):
    """Base error handler class.

    This class may be extended to support handling specific exception
    types.
    """

    def __init__(self, ctx: click.Context, ex: E):
        self.ctx = ctx
        self.ex = ex
        self.sc: SessionContext = self.ctx.obj  # type:ignore

    @abstractmethod
    def handle(self): ...


class HandleGretelResourceNotFoundError(_ErrorHandler, GretelResourceNotFound):
    """Handle errors for Gretel resources such as models, jobs
    and artifacts.
    """

    def handle(self):
        lines = [str(self.ex), "", "Context:"]
        if self.ex.context:
            for k, v in self.ex.context.items():
                lines.append(f"\t{k}: {v}")
        else:
            lines.append("\tNone")
        self.sc.log.error(("\n".join(lines)))
        self.ctx.exit(1)


class HandleApiClientError(_ErrorHandler, ApiException, ApiExceptionV1):
    """Handle Gretel API errors.

    Errors coming from the Gretel API have a ``context`` object that
    may be deserialized to give more details about the nature of the
    error.
    """

    def _build_from_context_item(self, s: str, context_item: dict) -> str:
        for field, errors in context_item.items():
            s += f"\t{field}\n"
            s += f"\t\t{errors}\n"
        return s

    def _context_to_str(self, err_body: dict) -> Optional[str]:
        context = err_body.get("context")
        if not context:
            return "\tNone"
        output = ""
        if isinstance(context, list):
            for context_item in context:
                output = self._build_from_context_item(output, context_item)
        else:
            output = self._build_from_context_item(output, context)
        return output

    def _get_error_message(self) -> str:
        if self.ex.status == 400:
            return "Bad API Request (400), please check your inputs."
        if self.ex.status == 401:
            return "API Request Unauthorized (401), please check your credentials."
        if self.ex.status == 404:
            return "The API resource was not found (404), please check your inputs."
        if self.ex.status == 403:
            return (
                "Your request may be rate limited, please try again later. "
                "If this problem persists please contact support."
            )
        return f"There was a problem with the API request ({self.ex.status})."

    def handle(self):
        err_sections = [f"{self._get_error_message()}"]
        try:
            err_body = json.loads(self.ex.body)
            err_sections.append(
                f'Reason: {err_body.get("message", ""), err_body.get("details", "")}',
                f"Context: \n{self._context_to_str(err_body)}",
            )
        except:
            err_sections.append(f"Reason: {self.ex.body}")
        self.sc.log.error("\n".join(err_sections))
        self.sc.exit(1)


class HandleConnectionError(_ErrorHandler, urllib3.exceptions.MaxRetryError):
    """Handles HTTP connection errors."""

    def handle(self):
        self.sc.log.error(f"Connection error.\nContext:\n\t{str(self.ex)}")
        self.sc.exit(1)


class HandlePythonError(_ErrorHandler, Exception):
    """Default exception handler.

    Any errors that aren't caught by a more specific handler
    will bubble up to this handler.
    """

    def handle(self):
        self.sc.log.error(str(self.ex))
        self.sc.exit(1)


class HandleClickExitNoop(_ErrorHandler, click.exceptions.Exit):
    def handle(self):
        self.sc.exit(self.ex.exit_code)


class ClickExceptionHandler(_ErrorHandler, click.ClickException):
    """Any click exceptions may be bubbled into the
    Click library for handling.
    """

    def handle(self):
        raise self.ex


def exception_map() -> Dict[Type[Exception], Type[_ErrorHandler]]:
    """Defines a map of exceptions to error handlers

    Note: Order matters.
    """
    return {
        ApiException: HandleApiClientError,
        ApiExceptionV1: HandleApiClientError,
        GretelResourceNotFound: HandleGretelResourceNotFoundError,
        urllib3.exceptions.MaxRetryError: HandleConnectionError,
        click.exceptions.Exit: HandleClickExitNoop,
        click.ClickException: ClickExceptionHandler,
        Exception: HandlePythonError,
    }


def handle_error(ex: Exception, ctx: click.Context):
    """Given a raised exception, this method will find a
    ``_ErrorHandler`` for handling the exception.

    Args:
        ex: The raised exception to handle
        ctx: Click context from the handler. This is passed into each
            error handler in case we need any additional context into
            how the error came to be.
    """

    if ctx.obj and ctx.obj.debug:
        ctx.obj.log.debug(traceback.format_exc())

    for ex_t, handler in exception_map().items():
        if isinstance(ex, ex_t):
            return handler(ctx, ex).handle()
    ctx.exit(1)


class ExceptionHandler(click.Group):
    """Wraps CLI command handlers in custom error
    handling logic.
    """

    def invoke(self, ctx: click.Context):
        try:
            super().invoke(ctx)
        except click.exceptions.Exit:
            pass
        except Exception as ex:
            handle_error(ex, ctx)
