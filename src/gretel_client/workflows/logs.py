"""
Various helpers for working with workflow related logs
"""

from __future__ import annotations

import datetime
import logging
import sys
import threading

from dataclasses import dataclass
from time import sleep
from typing import Callable, Iterator, List, Optional, Protocol, TextIO

from gretel_client.config import ClientConfig
from gretel_client.rest_v1.api.logs_api import LogsApi
from gretel_client.rest_v1.api.workflows_api import WorkflowsApi
from gretel_client.rest_v1.models import (
    GetLogResponse,
    LogEnvelope,
    SearchWorkflowTasksResponse,
    WorkflowTask,
)
from gretel_client.workflows.status import Status, TERMINAL_STATES

WORKFLOW_RUN_ID = "workflow_run_id"
"""Search key to lookup workflow runs"""

WORKFLOW_TASK_SEARCH_KEY = "workflowTask"
"""Key used to lookup logs for a workflow task"""

logger = logging.getLogger(__name__)


class WaitTimeExceeded(Exception):
    """
    Thrown when the wait time specified by the user has expired.
    """


class LogPrinter(Protocol):
    """Interface for printing workflow logs"""

    def info(self, msg: str):
        """Informational messages related to the workflow run or infrastructure"""
        ...

    def log(self, log_line: LogLine):
        """Log lines produced by each task"""
        ...

    def transition(self, task: Task):
        """Prints a message when a task transitions from one state to another"""
        ...


class StandardLogPrinter:
    """Implements a ``LogPrinter`` interface using stderr"""

    _fout: TextIO
    """File object to print logs and outputs to"""

    def __init__(self):
        self._fout = sys.stderr

    @classmethod
    def create(cls) -> StandardLogPrinter:
        return cls()

    def info(self, msg: str):
        print(f"INFO: {msg}", file=self._fout)

    def log(self, log_line: LogLine):
        print(f"[{log_line.group.name}] {log_line.ts} {log_line.msg}", file=self._fout)

    def transition(self, task: Task):
        print(f"[{task.name}] Task Status is now: {task.status}", file=self._fout)


@dataclass
class LogLine:
    """Parsed log line from the API. These logs are passed into the configured
    log printer for processing.
    """

    group: Task
    """Each log line is associated with a group, or task in this case."""
    ts: datetime.datetime
    """The timestamp the log line was produced"""

    msg: str
    """The contents of the log line"""

    @classmethod
    def from_envelope(cls, task: Task, envelope: LogEnvelope) -> LogLine:
        return cls(task, envelope.ts, envelope.msg)


@dataclass(frozen=True, eq=True)
class Task:
    """Represents a workflow run task"""

    name: str
    """The name of the task"""
    id: str
    """The id of the task"""

    status: str
    """The current status of the task"""

    error: Optional[str]
    """If the task failed with an error, this field will contain the associated
    error message.
    """

    did_transition: bool
    """If set, the task recently transitioned from a different status"""

    previous_status: Optional[str]
    """The previous status the task transitioned from"""

    @classmethod
    def from_api(cls, task: WorkflowTask) -> Task:
        """Create a new ``Task`` from a Gretel workflow task"""
        return cls(task.action_name, task.id, task.status, task.error_msg, False, None)

    @property
    def active(self) -> bool:
        """Returns ``True`` if the task is active and not in a terminal state"""
        return self.status not in TERMINAL_STATES

    def update(self, task: WorkflowTask) -> Task:
        """Updates the task from a Gretel API response"""
        return Task(
            task.action_name,
            task.id,
            task.status,
            task.error_msg,
            task.status != self.status,
            self.status,
        )


class TaskManager:
    """Monitors a workflow run for state changes and task logs."""

    _workflow_run_id: str
    """The id of the workflow run to monitor tasks from."""

    _tasks: dict[str, LogWorker]
    """Tasks associated with the workflow run. These task log workers are used
    to produce logs.
    """

    _workflows_api: WorkflowsApi
    """Workflows API client"""

    _logs_api: LogsApi
    """Logs API client"""

    _log_printer: LogPrinter
    """An instance of a log printer. Each workflow task worker will push log
    to this printer."""

    _poll_frequency_seconds: int = 5
    """The frequency between workflow run status checks"""

    def __init__(
        self,
        workflow_run_id: str,
        workflows_api: WorkflowsApi,
        logs_api: LogsApi,
        log_printer: LogPrinter,
    ):
        self._workflow_run_id = workflow_run_id
        self._workflows_api = workflows_api
        self._logs_api = logs_api
        self._log_printer = log_printer
        self._tasks = {}

    @classmethod
    def for_workflow_run(
        cls,
        workflow_run_id: str,
        workflow_api: WorkflowsApi,
        logs_api: LogsApi,
        log_printer: LogPrinter,
    ):
        """Create a ``TaskManager`` from a workflow run"""
        return cls(workflow_run_id, workflow_api, logs_api, log_printer)

    def start(self, wait: int = -1):
        """Starts polling the workflow run for state changes and task logs.
        This method will block until the workflow run reaches a terminal state
        of the wait threshold is exceeded.

        Args:
            wait: The time in seconds to wait before terminating the manager. If
                wait is ``-1`` this method will block until the workflow run
                reaches a terminal state.
        """
        wait_time_seconds = 0
        self._log_printer.info(
            f"Fetching task logs for workflow run {self._workflow_run_id}"
        )
        run_status = None
        while wait < 0 or wait_time_seconds < wait:
            for task in self._fetch_tasks():
                if task.id not in self._tasks:
                    self._log_printer.info(f"Got task {task.id}")
                    task_worker = LogWorker.for_workflow_task(
                        task, self._workflows_api, self._logs_api, self._log_printer
                    )
                    self._tasks[task.id] = task_worker
                    task_worker.start()

            new_run_status = self._check_run_status()
            if run_status != new_run_status:
                self._log_printer.info(
                    f"Workflow run is now in status: {new_run_status}"
                )
                run_status = new_run_status

            if run_status in TERMINAL_STATES:
                break

            wait_time_seconds += self._poll_frequency_seconds
            sleep(self._poll_frequency_seconds)

        if wait > 0 and wait_time_seconds >= wait:
            raise WaitTimeExceeded()

        # if wait is set, and we hit this branch, we've surpassed the allotted
        # wait time and don't want to wait for each log worker thread to terminate.
        if wait < 0:
            logger.debug("waiting for workers to terminate")
            self._wait_for_log_workers()

    def _check_run_status(self) -> str:
        workflow_run = self._workflows_api.get_workflow_run(
            workflow_run_id=self._workflow_run_id
        )
        return workflow_run.status

    def _fetch_tasks(self) -> List[WorkflowTask]:
        tasks: SearchWorkflowTasksResponse = self._workflows_api.search_workflow_tasks(
            query=f"{WORKFLOW_RUN_ID}:{self._workflow_run_id}"
        )
        return [t for t in tasks.tasks]

    def _wait_for_log_workers(self):
        while any([t.running() for t in self._tasks.values()]):
            sleep(1)


class LogWorker:
    """Binds to a task id and polls for logs. Log polling happens in a background
    thread with log messages being pushed into a configured log printer.

    The worker will automatically shutdown when the task reaches a terminal
    state.
    """

    _control: threading.Event
    """Controls the task thread. If the event is set, the task thread will
    terminate.
    """

    task: Task
    """The current task being polled"""

    _thread: threading.Thread
    """Thread to run the log poller from"""

    _logs_api: LogsApi
    """Used to fetch task logs"""

    _workflows_api: WorkflowsApi
    """Used to check the status of the task"""

    _poll_frequency_seconds: int = 3
    """Configures the time between state syncs with the Gretel api"""

    _log_printer: LogPrinter
    """Accepts log messages from the log thread"""

    _log_checkpoint: Optional[LogLine]
    """Used to dedupe already processed log lines."""

    def __init__(
        self,
        task: Task,
        workflows_api: WorkflowsApi,
        logs_api: LogsApi,
        log_printer: LogPrinter,
    ):
        self.task = task
        self._workflows_api = workflows_api
        self._logs_api = logs_api
        self._log_printer = log_printer
        self._control = threading.Event()
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._log_checkpoint = None

    @classmethod
    def for_workflow_task(
        cls,
        task: WorkflowTask,
        workflows_api: WorkflowsApi,
        logs_api: LogsApi,
        log_printer: LogPrinter,
    ) -> LogWorker:
        """Configures a log worker for a given workflow task"""
        return cls(Task.from_api(task), workflows_api, logs_api, log_printer)

    def running(self) -> bool:
        """Returns ``True`` if the worker is continuing to poll for logs."""
        return self._control.is_set()

    def start(self):
        """Starts the log worker."""
        self._control.set()
        self._thread.start()

    def stop(self):
        """Shutdowns the log worker."""
        self._control.clear()

    def wait(self, timeout: Optional[int] = None):
        """Wait until the log worker completes. This method will block until
        the task being polled reaches a terminal state or the timeout is hit.


        Args:
            timeout: The max time in seconds to wait for the worker to complete.
        """
        self._thread.join(timeout)

    def _fetch_log_lines(
        self, next_page_token: Optional[str] = None
    ) -> Iterator[LogLine]:
        params = {
            "query": f"{WORKFLOW_TASK_SEARCH_KEY}:{self.task.id}",
            "limit": 1_000,
        }

        if next_page_token is not None:
            params["page_token"] = next_page_token

        try:
            resp: GetLogResponse = self._logs_api.get_logs(**params)
        except Exception as ex:
            logger.debug(f"got error fetching logs: {ex}")
            return

        line_envelope: LogEnvelope
        for line_envelope in resp.lines:
            log_line = LogLine.from_envelope(self.task, line_envelope)

            self._log_checkpoint = log_line
            yield self._log_checkpoint

        # We get a continuation token no matter what. If the current response
        # has no log lines, we can assume we've exhausted the current log
        # cursor.
        if len(resp.lines) > 0:
            yield from self._fetch_log_lines(resp.next_page_token)

    def _poll(self):
        while self._control.is_set():
            for line in self._fetch_log_lines():
                self._log_printer.log(line)

            self._sync_task()

            if self.task.did_transition:
                self._log_printer.transition(self.task)

            if not self.task.active:
                if self.task.error != "":
                    self._log_printer.info(
                        f"Task {self.task.name} has error: {self.task.error}"
                    )
                self._control.clear()
            else:
                sleep(self._poll_frequency_seconds)

    def _sync_task(self):
        """Syncs tasks state with the API."""
        task: WorkflowTask = self._workflows_api.get_workflow_task(
            workflow_task_id=self.task.id
        )
        self.task = self.task.update(task)


LogPrinterFactory = Callable[..., LogPrinter]


def print_logs_for_workflow_run(
    id: str,
    config: ClientConfig,
    log_printer_factory: LogPrinterFactory = StandardLogPrinter.create,
    wait: int = -1,
):
    """Prints task logs for a workflow run. This method with block until the
    workflow run reaches a terminal state.

    Args:
        id: The workflow run id to fetch logs for.
        config: Client config used to configure API client
            bindings from.
        log_printer_factory: Factory method to create an instance of a
            ``LogPrinter``. This printer will be used to emit logs for the
            workflow run and associated tasks.
        wait: The number of seconds to wait before terminating the log
            poller. If set to ``-1`` the poller will wait indefinitely until
            the workflow run reaches a terminal state.
    """
    workflows_api = config.get_v1_api(WorkflowsApi)
    logs_api = config.get_v1_api(LogsApi)

    task_manager = TaskManager.for_workflow_run(
        id, workflows_api, logs_api, log_printer_factory()
    )

    task_manager.start(wait)
