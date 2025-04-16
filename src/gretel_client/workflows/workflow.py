import io
import logging

from typing import IO, Literal, Optional, Union

from requests import HTTPError
from typing_extensions import Self

from gretel_client.navigator_client_protocols import (
    GretelApiProviderProtocol,
    GretelResourceProviderProtocol,
)
from gretel_client.rest_v1.api.logs_api import LogsApi
from gretel_client.rest_v1.api.workflows_api import WorkflowsApi
from gretel_client.rest_v1.models import WorkflowRun as WorkflowRunApiResponse
from gretel_client.workflows.configs.workflows import Step, Workflow
from gretel_client.workflows.io import Dataset, PydanticModel, Report
from gretel_client.workflows.logs import LogLine, LogPrinter, Task, TaskManager


class WorkflowRun:
    """
    The WorkflowRun class represents a concrete execution of a Workflow,
    providing methods to monitor execution, retrieve outputs, and access logs.
    Each workflow execution is composed of :class:`steps<gretel_client.workflows.configs.workflows.Step>`
    that form a directed acyclic graph (DAG).

    You should never directly instantiate a WorkflowRun, but instead use
    Workflow methods from the main :class:`Gretel<gretel_client.navigator_client.Gretel>`
    class.
    """

    def __init__(
        self,
        workflow: WorkflowRunApiResponse,
        api_provider: GretelApiProviderProtocol,
        resource_provider: GretelResourceProviderProtocol,
    ) -> None:
        self._api_provider = api_provider
        self._workflow_api = api_provider.get_api(WorkflowsApi)
        self._logs_api = api_provider.get_api(LogsApi)
        self._api_response = workflow
        self._resource_provider = resource_provider

    @classmethod
    def from_workflow_run_id(
        cls,
        workflow_run_id: str,
        api_provider: GretelApiProviderProtocol,
        resource_provider: GretelResourceProviderProtocol,
    ) -> Self:
        workflow = api_provider.get_api(WorkflowsApi).get_workflow_run(
            workflow_run_id=workflow_run_id
        )
        return cls(workflow, api_provider, resource_provider)

    def wait_until_done(
        self,
        wait: int = -1,
        verbose: bool = True,
        log_printer: Optional[LogPrinter] = None,
    ):
        """
        Wait for the workflow run to complete, with optional logging.

        Args:
            wait: Maximum time to wait in seconds. -1 means wait indefinitely
            verbose: Whether to print detailed logs during execution
            log_printer: Custom log printer implementation. If None, uses LoggingPrinter
        """
        if not log_printer:
            log_printer = LoggingPrinter(verbose)

        task_manager = TaskManager(
            self._api_response.id, self._workflow_api, self._logs_api, log_printer
        )

        task_manager.start(wait)

    def get_step_output(
        self, step_name: str, format: Optional[str] = None
    ) -> Union[PydanticModel, Dataset, Report, IO]:
        """
        Retrieve the output from a specific workflow step.

        Args:
            step_name: Name of the workflow step
            format: Optional output format specification

        Returns:
            The step output in the appropriate format (PydanticModel, Dataset, Report, or IO)

        Raises:
            Exception: If the step cannot be found or output type cannot be determined
        """
        output_type = None
        endpoint = f"/v2/workflows/runs/{self.id}/{step_name}/outputs"
        params = {}
        if format:
            params["format"] = format
        else:
            # First find the task type for the step name
            step_type = None
            if self.workflow.steps:
                for step in self.workflow.steps:
                    if step_name == step.name:
                        step_type = step.task

            if not step_type:
                raise Exception(f"Could not find step {step_name!r} in workflow")

            # Next use the registry to lookup the output type
            # for the task.
            tasks: list[dict] = self._resource_provider.workflows.registry()["tasks"]
            for task in tasks:
                if task["name"] == step_type:
                    output_type = task["output"]

            if not output_type:
                raise Exception(
                    f"Could not determine output type for step {step_type!r}"
                )

        with self._api_provider.requests().get(
            endpoint, params=params, stream=True
        ) as response:
            response.raise_for_status()
            response_bytes = io.BytesIO(response.content)

            # if a format is specified, we assume the caller know what
            # format it wants, and should return the raw bytes
            if not output_type and format:
                return response_bytes

            # otherwise, if there is an output_type found, try and
            # serialize the output to that type
            if output_type == "dataset":
                return Dataset.from_bytes(response_bytes)

            # todo: this needs to be more flexible. we should lookup
            # tasks that emit some sort of report base class from
            # the registry.
            if output_type in ("evaluate_safe_synthetics_dataset", "evaluate_dataset"):
                return Report.from_bytes(response_bytes, self._download_report)

            return PydanticModel.from_bytes(response_bytes)

    @property
    def name(self) -> str:
        """Get the name of the Workflow"""
        return self.workflow.name

    @property
    def workflow(self) -> Workflow:
        """Get the Workflow configuration"""
        return Workflow(**self._api_response.config or {})

    @property
    def id(self) -> str:
        """Get the ID of the Workflow Run"""
        return self._api_response.id

    @property
    def workflow_id(self) -> str:
        """Get the ID of the parent Workflow"""
        return self._api_response.workflow_id

    @property
    def steps(self) -> list[Step]:
        """Return a list of steps in the Workflow"""
        return self.workflow.steps or []

    @property
    def config(self) -> dict:
        """Return the Workflow config as a dictionary"""
        return self._api_response.config or {}

    @property
    def config_yaml(self) -> str:
        """Return the Workflow config as yaml"""
        return self._api_response.config_text or ""

    @property
    def report(self) -> Report:
        """Return the report for the Workflow if one exists"""
        return Report.from_bytes(
            self._download_report(format="json"), self._download_report
        )

    def _download_report(
        self, format: Optional[Literal["json", "html"]] = "json"
    ) -> IO:
        with self._api_provider.requests().get(
            f"/v2/workflows/runs/{self.id}/outputs?type=report_{format}",
            stream=True,
        ) as response:
            try:
                response.raise_for_status()
            except HTTPError as ex:
                if ex.response.status_code == 404:
                    raise Exception(
                        "Could not fetch a report for the task. "
                        "Please check that the workflow has a report task"
                    )
                raise ex
            return io.BytesIO(response.content)

    @property
    def dataset(self) -> Dataset:
        """Get the final output Dataset of the Workflow if one exists"""
        with self._api_provider.requests().get(
            f"/v2/workflows/runs/{self.id}/outputs?type=dataset_parquet",
            stream=True,
        ) as response:
            try:
                response.raise_for_status()
            except HTTPError as ex:
                if ex.response.status_code == 404:
                    raise Exception(
                        "Could not fetch a dataset for the task. "
                        "Please check that the workflow has a dataset "
                        "producing task."
                    )
                raise ex
            return Dataset.from_bytes(io.BytesIO(response.content))

    @property
    def console_url(self) -> str:
        """
        Get the URL for viewing this Workflow Run in the Gretel Console.
        """
        return (
            f"{self._resource_provider.console_url}/workflows/"
            f"{self._api_response.workflow_id}/runs/{self._api_response.id}"
        )


class LoggingPrinter:
    def __init__(self, verbose: bool = True):
        self.logger = logging.getLogger(__name__)
        self.verbose = verbose

    @classmethod
    def create(cls) -> Self:
        return cls()

    def info(self, msg: str) -> None:
        if self.verbose:
            self.logger.info(msg)

    def log(self, log_line: LogLine) -> None:
        if self.verbose:
            self.logger.info(f"[{log_line.group.name}] {log_line.ts} {log_line.msg}")

    def transition(self, task: Task) -> None:
        self.logger.info(f"[{task.name}] Task Status is now: {task.status}")
