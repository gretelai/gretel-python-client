from __future__ import annotations

import json
import logging

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterator, Optional, Union

import pandas as pd
import requests
import yaml

from pydantic import BaseModel
from requests.exceptions import ChunkedEncodingError
from typing_extensions import Self

from gretel_client._api.api.default_api import DefaultApi, TaskEnvelope
from gretel_client._api.api_client import ApiException
from gretel_client._api.models.exec_batch_request import ExecBatchRequest
from gretel_client._api.models.workflow_input import WorkflowInput
from gretel_client.errors import (
    BROKEN_RESPONSE_STREAM_ERROR_MESSAGE,
    NavigatorApiClientError,
    NavigatorApiServerError,
)
from gretel_client.files.interface import File
from gretel_client.navigator_client_protocols import (
    GretelApiProviderProtocol,
    GretelResourceProviderProtocol,
)
from gretel_client.rest_v1.api.workflows_api import WorkflowsApi
from gretel_client.workflows.configs.workflows import Globals, Step, Workflow
from gretel_client.workflows.tasks import task_to_step, TaskConfig
from gretel_client.workflows.workflow import WorkflowRun

logger = logging.getLogger(__name__)


class WorkflowTaskError(Exception):
    """
    Represents an error returned by the Task. This error
    is most likely related to an issue with the Task
    itself. If you see this error check your Task config
    first. If the issue persists, the error might be a bug
    in the remote Task implementation.
    """


class FieldViolation(BaseModel):
    """Represent a field that has failed schema validation"""

    field: str
    error_message: str

    @staticmethod
    def from_api_response(field_violations: list[dict]) -> list[FieldViolation]:
        return [FieldViolation(**violation) for violation in field_violations]


class WorkflowValidationError(Exception):
    """
    Raised when workflow schema validation fails.

    Use field_violations to access validation errors by field name.
    """

    def __init__(self, msg, field_violations: Optional[list[FieldViolation]] = None):
        super().__init__(msg)
        self._field_violations = field_violations or []

    @property
    def field_violations(self) -> list[FieldViolation]:
        return self._field_violations


@dataclass
class WorkflowInterruption:
    """
    Provide a user friendly error message when a workflow is
    unexpectedly interrupted.
    """

    message: str


@dataclass
class Message:

    step: str
    """The name of the step"""

    stream: str
    """
    The stream the message should be associated with.

    We use multiple streams so that we can differentiate between different types of outputs.
    """

    payload: dict
    """The actual value of the output"""

    type: str
    """The type of message"""

    ts: datetime
    """The date and time the message was created"""

    @classmethod
    def from_dict(cls, message: dict, raise_on_error: bool = False) -> Message:
        message["ts"] = datetime.fromisoformat(message["ts"])
        deserialized_message = cls(**message)

        if raise_on_error:
            _raise_on_task_error(deserialized_message)

        return deserialized_message


def _default_preview_printer(log: Union[Message, WorkflowInterruption]):
    if isinstance(log, Message) and log.payload:
        if log.stream == "logs" and log.payload.get("msg"):
            logger.info(f"[{log.step}] {log.payload.get('msg')}")
        if log.stream == "step_outputs":
            logger.info(f"[{log.step}] {log.payload}")
        if isinstance(log, WorkflowInterruption):
            logger.error(log)


class WorkflowSessionManager:

    def __init__(self):
        self._workflow_id = None

    def set_id(self, workflow_id: Optional[str]):
        self._workflow_id = workflow_id

    def get_id(self) -> Optional[str]:
        return self._workflow_id


class WorkflowBuilder:
    """
    A builder class for creating Gretel workflows.

    This class provides a fluent interface for constructing Workflow objects
    by chaining method calls. It allows setting the workflow name and adding
    steps sequentially.
    """

    _workflow_api: WorkflowsApi

    def __init__(
        self,
        project_id: str,
        api_provider: GretelApiProviderProtocol,
        resource_provider: GretelResourceProviderProtocol,
        workflow_session_manager: Optional[WorkflowSessionManager] = None,
    ) -> None:
        """Initialize a new WorkflowBuilder with empty name and steps."""
        self._api_provider = api_provider
        self._resource_provider = resource_provider
        self._project_id = project_id
        self._workflow_api = self._api_provider.get_api(WorkflowsApi)
        self._data_api = self._api_provider.get_api(DefaultApi)

        if not workflow_session_manager:
            workflow_session_manager = WorkflowSessionManager()
        self._workflow_session_manager = workflow_session_manager

        # fields managed by the builder to configure the workflow
        self._name = ""
        self._input_file_id = None
        self._steps: list[Step] = []
        self._globals = Globals()
        self._inputs = None

    def set_name(self, name: str) -> Self:
        """Set the name of the workflow.

        Args:
            name: The name to assign to the workflow.

        Returns:
            Self: The builder instance for method chaining.
        """
        self._name = name
        return self

    def for_workflow(self, workflow_id: Optional[str] = None) -> Self:
        """Configure this builder to use an existing workflow.

        When a workflow ID is specified, the `run()` method will execute a new run
        within the context of the existing workflow instead of creating a new workflow.
        This allows multiple runs to share the same workflow.

        Args:
            workflow_id: The ID of an existing workflow to use. If set to
                None, a new workflow will get created for the subsequent
                run.

        Returns:
            Self: The builder instance for method chaining
        """
        self._workflow_session_manager.set_id(workflow_id)
        return self

    def with_data_source(
        self,
        data_source: Union[str, Path, pd.DataFrame, File],
        purpose: str = "dataset",
    ) -> Self:
        """Add a data source to the workflow.

        This method allows you to specify the primary data input for your workflow.
        The data source will be connected to the first step in the workflow chain.

        Args:
            data_source: The data to use as input. Can be one of:
                - A File object from the Gretel SDK
                - A file ID string (starting with "file_")
                - A path to a local file (string or Path)
                - A pandas DataFrame
            purpose: The purpose tag for the uploaded data. Defaults to "dataset".

        Returns:
            Self: The builder instance for method chaining.

        Examples:
            # Using a pandas DataFrame
            builder.with_data_source(df)

            # Using a file path
            builder.with_data_source("path/to/data.csv")

            # Using an existing Gretel File object
            builder.with_data_source(file_obj)

            # Using a file ID
            builder.with_data_source("file_abc123")
        """
        if isinstance(data_source, File):
            self._input_file_id = data_source.id

        elif isinstance(data_source, str) and data_source.startswith("file_"):
            self._input_file_id = data_source

        else:
            file = self._resource_provider.files.upload(data_source, purpose)
            self._input_file_id = file.id

        return self

    @property
    def data_source(self) -> Optional[str]:
        return self._input_file_id

    def add_step(
        self,
        step: Union[TaskConfig, Step],
        step_inputs: Optional[list[str]] = None,
        validate: bool = True,
    ) -> Self:
        """Add a single step to the workflow.

        Args:
            step: The workflow step to add.
            step_input: Configure an input for the step or task.
            validate: Whether to validate the step. Defaults to True.

        Returns:
            Self: The builder instance for method chaining.
        """
        if isinstance(step, TaskConfig) and not isinstance(step, Step):
            step = task_to_step(step)

        for existing_step in self._steps:
            if existing_step.name == step.name:
                new_existing_name, new_step_name = _disambiguate_name(
                    existing_step.name, step.name, [s.name for s in self._steps]
                )

                existing_step.name = new_existing_name
                step.name = new_step_name

        if validate:
            self.validate_step(step)

        if step_inputs:
            step.inputs = step_inputs

        self._steps.append(step)
        return self

    def validate_step(self, step: Step) -> str:
        """Validate a workflow step using the Gretel API.

        This method makes an API call to validate the configuration of a workflow step
        before adding it to the workflow. It ensures the task type and configuration
        are valid according to the Gretel platform's requirements.

        Args:
            step: The workflow step to validate, containing task name and configuration.

        Returns:
            str: Validation message if successful. Empty string if no message was returned.

        Raises:
            WorkflowValidationError: If the step fails validation. The exception includes
                field-specific violations that can be accessed via the field_violations
                property.
            ApiException: If there is an issue with the API call not related to validation.
        """
        try:
            resp = self._data_api.tasks_validate_v2_workflows_tasks_validate_post(
                TaskEnvelope(name=step.task, config=step.config)
            )
        except ApiException as ex:
            if ex.status == 422 and ex.body:
                body = json.loads(ex.body)
                details = body.get("details", [])
                field_violations = []
                if len(details) == 1:
                    field_violations = FieldViolation.from_api_response(
                        details[0].get("field_violations", [])
                    )

                message = body.get("message")
                logger.error(
                    f"{message}: task: {step.task!r} step: {step.name!r}:",
                )

                for violation in field_violations:
                    logger.error(f"\t{violation}")

                raise WorkflowValidationError(body.get("message"), field_violations)
            raise ex

        if not resp.valid:
            raise WorkflowValidationError(
                f"Task {step.task!r} is not valid: {resp.message}"
            )

        return resp.message if resp.message else ""

    def add_steps(
        self, steps: list[Union[TaskConfig, Step]], validate: bool = True
    ) -> Self:
        """Add multiple steps to the workflow.

        Args:
            steps: A list of workflow steps to add.
            validate: Whether to validate the steps. Defaults to True.

        Returns:
            Self: The builder instance for method chaining.
        """
        for step in steps:
            self.add_step(step, validate=validate)
        return self

    def to_workflow(self) -> Workflow:
        """Convert the builder to a Workflow object.

        Returns:
            Workflow: A new Workflow instance with the configured name and steps.
        """
        if self._input_file_id and len(self._steps) > 0:
            existing_inputs = self._steps[0].inputs or []
            self._steps[0].inputs = list(set(existing_inputs + [self._input_file_id]))

        return Workflow(
            name=(
                self._name if self._name != "" else _generate_workflow_name(self._steps)
            ),
            steps=_autowire_steps(self._steps),
        )

    def to_dict(self) -> dict:
        """Convert the workflow to a dictionary representation.

        Returns:
            dict: A dictionary representation of the workflow.
        """
        return self.to_workflow().model_dump(exclude_unset=True)

    def to_yaml(self) -> str:
        """Convert the workflow to a YAML string representation.

        Returns:
            str: A YAML string representation of the workflow.
        """
        return yaml.dump(self.to_dict(), sort_keys=False)

    def iter_preview(self) -> Iterator[Union[Message, WorkflowInterruption]]:
        """Stream workflow execution messages for preview purposes.

        This method executes the workflow in streaming preview mode, returning
        an iterator that yields messages as they are received from the workflow
        execution. This allows for real-time monitoring of workflow execution
        before you submit your workflow for batch execution.

        Returns:
            Iterator[Union[Message, WorkflowInterruption]]: An iterator that yields:
                - Message objects containing logs, outputs, and state changes from the workflow
                - WorkflowInterruption if the stream is unexpectedly disconnected
        """
        with self._api_provider.requests().post(
            "/v2/workflows/exec_streaming",
            json=WorkflowInput(**self.to_dict()).model_dump(exclude_unset=True),
            stream=True,
        ) as response:
            _check_for_error_response(response)
            try:
                for output in response.iter_lines():
                    try:
                        if isinstance(output, bytes):
                            output = output.decode("utf-8")
                        yield Message.from_dict(json.loads(output), raise_on_error=True)
                    except json.JSONDecodeError as e:
                        logger.error(f"Could not deserialize message: {output}", e)
            except ChunkedEncodingError:
                yield WorkflowInterruption(BROKEN_RESPONSE_STREAM_ERROR_MESSAGE)
            except WorkflowTaskError as exc:
                yield WorkflowInterruption(str(exc))

    def preview(
        self,
        log_printer: Callable[
            [Union[Message, WorkflowInterruption]], None
        ] = _default_preview_printer,
    ):
        """Preview the workflow in realtime.

        Args:
            log_printer: A callable that processes each message or interruption.
                Defaults to _default_preview_printer which logs messages to the console
                in a human-readable format. You can provide your own function to
                customize how messages are processed.
        """
        for log in self.iter_preview():
            log_printer(log)

    def run(self, name: Optional[str] = None, wait: bool = False) -> WorkflowRun:
        """Execute the workflow as a batch job.

        This method creates a persistent workflow and runs it as a batch job on
        the Gretel platform. Unlike preview, this creates a permanent record
        of the workflow execution that can be referenced later.

        Args:
            name: Optional name to assign to the workflow. If provided,
                this will override any name previously set with the name() method.

        Returns:
            WorkflowRun: A WorkflowRun object representing the running workflow.
                This can be used to track the status of the workflow and retrieve
                results when it completes.
        """
        if name:
            self.set_name(name)

        # todo: get rid of this model and instead use a dict
        # from the api route definition
        workflow_config = WorkflowInput(**self.to_dict())

        try:
            response = self._data_api.workflows_exec_batch_v2_workflows_exec_batch_post(
                ExecBatchRequest(
                    project_id=self._project_id,
                    workflow_config=workflow_config,
                    workflow_id=self._workflow_session_manager.get_id(),
                )
            )

        except ApiException as ex:
            try:
                error_details = json.loads(ex.body)
                logger.error(f"Could not submit workflow: {error_details}")
            except json.JSONDecodeError:
                logger.error(f"Could not decode response, raw error body: {ex.body}")
            raise ex

        wf_op = "Creating" if not self._workflow_session_manager.get_id() else "Using"
        logger.info(f"▶️ {wf_op} Workflow: {response.workflow_id}")
        logger.info(f"▶️ Created Workflow Run: {response.workflow_run_id}")

        self._workflow_session_manager.set_id(response.workflow_id)

        workflow_run = WorkflowRun.from_workflow_run_id(
            response.workflow_run_id,
            self._api_provider,
            self._resource_provider,
        )

        logger.info(f"🔗 Workflow Run console link: {workflow_run.console_url}")

        if wait:
            workflow_run.poll()

        return workflow_run


def _check_for_error_response(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError:
        if 400 <= response.status_code < 500:
            raise NavigatorApiClientError(response.json())
        else:
            raise NavigatorApiServerError(response.json())


def _raise_on_task_error(message: Message):
    """
    Inspects a message for messages known to contain
    fatal errors, and raises an exception from those
    messages.
    """
    if (
        message.type == "step_state_change"
        and message.payload.get("state", "") == "error"
    ):
        raise WorkflowTaskError(
            f"Step {message.step!r} failed: {message.payload.get('msg')}. Please check your Workflow config. "
            "If the issue persists please contact support."
        )


def _generate_workflow_name(steps: Optional[list[Step]] = None) -> str:
    """Try and generate a default workflow name.

    If there are steps in the workflow, we try and generate a name
    based on the steps.
    """
    date_str = datetime.now().strftime("%Y-%m-%d")
    step_str = "my-workflows"
    if steps:
        if len(steps) == 1:
            step_str = f"{steps[0].name}--"
        else:
            step_str = f"{steps[-2].name}--{steps[-1].name}--"

    return f"{step_str}{date_str}"


# todo: remove me once backend impl lands
def _autowire_steps(steps: list[Step]) -> list[Step]:
    prev_step = None
    for step in steps:
        if prev_step:
            inputs = step.inputs or []
            if len(inputs) == 0:
                step.inputs = inputs + [prev_step.name]
        prev_step = step

    return steps


def _disambiguate_name(
    step_one: str, step_two: str, all_names: list[str]
) -> tuple[str, str]:
    """Disambiguate conflicting step names by adding numeric suffixes."""

    def get_unique_name(base_name: str) -> str:
        counter = 1
        new_name = f"{base_name}-{counter}"

        # Keep incrementing suffix until we find a name not in use
        while new_name in all_names:
            counter += 1
            new_name = f"{base_name}-{counter}"

        return new_name

    # If the names are already different, no need to disambiguate
    if step_one != step_two:
        return step_one, step_two

    new_step_one = get_unique_name(step_one)
    all_names.append(new_step_one)
    new_step_two = get_unique_name(step_one)

    return new_step_one, new_step_two
