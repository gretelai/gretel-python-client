from __future__ import annotations

import json
import logging
import random
import string

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

import pandas as pd
import requests
import yaml

from pydantic import BaseModel
from requests.exceptions import ChunkedEncodingError
from typing_extensions import Self

from gretel_client._api.api.workflows_api import WorkflowsApi as V2WorkflowsApi
from gretel_client._api.api_client import ApiException
from gretel_client._api.models.exec_batch_request import ExecBatchRequest
from gretel_client._api.models.task_envelope_for_validation import (
    TaskEnvelopeForValidation,
)
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
from gretel_client.workflows.configs.tasks import DataSource
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

    def __str__(self) -> str:
        return f"{self.field}: {self.error_message}"


class WorkflowValidationError(Exception):
    """
    Raised when workflow schema validation fails.

    Use field_violations to access validation errors by field name.
    """

    def __init__(
        self,
        msg: str,
        *,
        task_name: str | None = None,
        step_name: str | None = None,
        field_violations: list[FieldViolation] | None = None,
    ):
        super().__init__(msg)
        self._msg = msg
        self._field_violations = field_violations or []
        self._task_name = task_name
        self._step_name = step_name

    @property
    def msg(self) -> str:
        return self._msg

    @property
    def field_violations(self) -> list[FieldViolation]:
        return self._field_violations

    @property
    def task_name(self) -> str:
        return self._task_name or ""

    @property
    def step_name(self) -> str:
        return self._step_name or ""

    def __str__(self) -> str:
        violations = "\n".join(str(fv) for fv in self._field_violations)
        return f"{super().__str__()}\n{violations}"


@dataclass
class WorkflowInterruption:
    """
    Provide a user friendly error message when a workflow is
    unexpectedly interrupted.
    """

    message: str


@dataclass
class LogMessage:
    level: str
    msg: str

    @property
    def is_error(self) -> bool:
        return self.level == "error"

    @property
    def is_info(self) -> bool:
        return self.level == "info"

    @property
    def is_warning(self) -> bool:
        return self.level == "warn"


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
            deserialized_message.raise_for_error()

        return deserialized_message

    @property
    def has_log_message(self) -> bool:
        return self.stream == "logs" and "msg" in self.payload

    @property
    def log_message(self) -> LogMessage | None:
        if self.has_log_message:
            return LogMessage(**self.payload)
        return None

    @property
    def has_output(self) -> bool:
        return self.stream == "step_outputs"

    @property
    def has_dataset(self) -> bool:
        return self.has_output and "dataset" in self.payload

    @property
    def dataset(self) -> pd.DataFrame:
        records = []
        if self.has_dataset:
            records = self.payload["dataset"]
        return pd.DataFrame.from_records(records)

    def raise_for_error(self) -> None:
        """Check for fatal errors and raise an exception if found."""
        if (
            self.type == "step_state_change"
            and self.payload.get("state", "") == "error"
        ):
            raise WorkflowTaskError(
                f"Step {self.step!r} failed: "
                f"{self.payload.get('msg', '').strip(' .')}. "
                "Please check your Workflow config. "
                "If the issue persists please contact support."
            )


def _default_preview_printer(log: Message | WorkflowInterruption):
    if isinstance(log, Message) and log.payload:
        if log.has_log_message:
            logger.info(f"[{log.step}] {log.log_message.msg}")
        if log.has_output:
            logger.info(f"[{log.step}] {log.payload}")
    if isinstance(log, WorkflowInterruption):
        logger.error(f"{log.message}")


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
        globals: Globals,
        api_provider: GretelApiProviderProtocol,
        resource_provider: GretelResourceProviderProtocol,
        workflow_session_manager: WorkflowSessionManager | None = None,
    ) -> None:
        self._api_provider = api_provider
        self._resource_provider = resource_provider
        self._project_id = project_id
        self._workflow_api = self._api_provider.get_api(WorkflowsApi)
        self._data_api = self._api_provider.get_api(V2WorkflowsApi)

        if not workflow_session_manager:
            workflow_session_manager = WorkflowSessionManager()
        self._workflow_session_manager = workflow_session_manager

        # fields managed by the builder to configure the workflow
        self._name = ""
        self._globals = globals

        # inputs and data sources
        self._inputs = None
        self._input_file_id = None
        self._use_data_source_step = None

        # steps
        self._steps: list[Step] = []
        self._step_hash_to_name_map: dict[int, str] = {}

    @property
    def step_names(self) -> list[str]:
        """Return a list of step names for the current builder"""
        return [step.name for step in self._steps]

    def set_name(self, name: str) -> Self:
        """
        Set the name of the workflow.

        Args:
            name: The name to assign to the workflow.

        Returns:
            Self: The builder instance for method chaining.
        """
        self._name = name
        return self

    def for_workflow(self, workflow_id: str | None = None) -> Self:
        """
        Configure this builder to use an existing workflow.

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
        data_source: str | Path | pd.DataFrame | File,
        purpose: str = "dataset",
        use_data_source_step: bool = False,
    ) -> Self:
        """
        Add a data source to the workflow.

        This method allows you to specify the primary data input for your workflow.
        The data source will be connected to the first step in the workflow chain.

        Args:
            data_source: The data to use as input. Can be one of:
                - A File object from the Gretel SDK
                - A file ID string (starting with "file_")
                - A path to a local file (string or Path)
                - A pandas DataFrame
            purpose: The purpose tag for the uploaded data. Defaults to "dataset".
            use_data_source_step: Instead of passing a file_id as an input, use
                the DataSource task. Generally you shouldn't need to set this.

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
        self._use_data_source_step = use_data_source_step
        self._input_file_id = self.prepare_data_source(data_source)

        if self._name == "":
            if isinstance(data_source, str):
                self._name = Path(data_source).stem

            if isinstance(data_source, Path):
                self._name = data_source.stem

            if isinstance(data_source, File):
                self._name = data_source.filename

            if self._name != "":
                self._name = f"{self._name}-{_generate_random_string()}"

        return self

    def prepare_data_source(
        self,
        data_source: str | Path | pd.DataFrame | File,
        purpose: str = "dataset",
    ) -> str:
        """
        Uploads the data source to the Files API if it is not already a File
        and returns the file ID.
        """
        if isinstance(data_source, File):
            return data_source.id

        elif isinstance(data_source, str) and data_source.startswith("file_"):
            return data_source

        else:
            file = self._resource_provider.files.upload(data_source, purpose)
            return file.id

    @property
    def data_source(self) -> str | None:
        """Return the current input data source for the builder."""
        if self._use_data_source_step:
            return "read-data-source"
        return self._input_file_id

    def add_step(
        self,
        step: TaskConfig | Step,
        step_inputs: list[TaskConfig | Step | str] | None = None,
        validate: bool = True,
        step_name: str | None = None,
    ) -> Self:
        """
        Add a single step to the workflow.

        Args:
            step: The workflow step to add.
            step_input: Configure an input for the step or task.
            validate: Whether to validate the step. Defaults to True.
            step_name: The name of the step. If not provided, the name will be
                generated based on the name of the task.

        Returns:
            Self: The builder instance for method chaining.
        """
        # uniquely identify step or task config objects
        hash_before_transform = _get_object_hash(step)
        disambiguated_step_input_names = self._disambiguate_step_inputs(
            step_inputs=step_inputs
        )
        if isinstance(step, TaskConfig) and not isinstance(step, Step):
            step = task_to_step(step, inputs=disambiguated_step_input_names)
        else:
            step.inputs = disambiguated_step_input_names

        if step_name is not None:
            step.name = step_name

        step = self._ensure_unique_step_name(step)

        if validate:
            self.validate_step(step)

        self._steps.append(step)
        self._step_hash_to_name_map[hash_before_transform] = step.name
        return self

    def validate_step(self, step: Step) -> str:
        """
        Validate a workflow step using the Gretel API.

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
            resp = self._data_api.validate_workflow_task(
                TaskEnvelopeForValidation(
                    name=step.task,
                    globals=self._globals.model_dump(),
                    config=step.config,
                )
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
                raise WorkflowValidationError(
                    message,
                    task_name=step.task,
                    step_name=step.name,
                    field_violations=field_violations,
                )
            raise ex

        if not resp.valid:
            raise WorkflowValidationError(
                f"Task {step.task!r} is not valid: {resp.message}",
                task_name=step.task,
                step_name=step.name,
            )

        return resp.message if resp.message else ""

    def add_steps(self, steps: list[TaskConfig | Step], validate: bool = True) -> Self:
        """
        Add multiple steps to the workflow.

        Args:
            steps: A list of workflow steps to add.
            validate: Whether to validate the steps. Defaults to True.

        Returns:
            Self: The builder instance for method chaining.
        """
        for step in steps:
            self.add_step(step, validate=validate)
        return self

    def get_steps(self) -> list[Step]:
        """Return the list of steps in the workflow."""
        return self._steps

    def get_step(self, step_name: str) -> Step | None:
        for step in self._steps:
            if step.name == step_name:
                return step

    def to_workflow(self) -> Workflow:
        """
        Convert the builder to a Workflow object.

        Returns:
            Workflow: A new Workflow instance with the configured name and steps.
        """
        if (
            self._input_file_id
            and len(self._steps) > 0
            and not self._use_data_source_step
        ):
            existing_inputs = self._steps[0].inputs or []
            self._steps[0].inputs = list(set(existing_inputs + [self._input_file_id]))

        if self._use_data_source_step and self._input_file_id:
            if len(self._steps) > 0 and self._steps[0].name == "read-data-source":
                pass
            else:
                self._steps.insert(
                    0,
                    Step(
                        name="read-data-source",
                        task="data_source",
                        config=DataSource(data_source=self._input_file_id).model_dump(),
                    ),
                )

        return Workflow(
            name=(
                self._name if self._name != "" else _generate_workflow_name(self._steps)
            ),
            globals=self._globals,
            steps=self._steps,
        )

    def to_dict(self) -> dict:
        """
        Convert the workflow to a dictionary representation.

        Returns:
            dict: A dictionary representation of the workflow.
        """
        return self.to_workflow().model_dump(exclude_unset=True)

    def to_yaml(self) -> str:
        """
        Convert the workflow to a YAML string representation.

        Returns:
            str: A YAML string representation of the workflow.
        """
        return yaml.dump(self.to_dict(), sort_keys=False)

    def iter_preview(self) -> Iterator[Message | WorkflowInterruption]:
        """
        Stream workflow execution messages for preview purposes.

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
            json=self.to_dict(),
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
            [Message | WorkflowInterruption], None
        ] = _default_preview_printer,
    ):
        """
        Preview the workflow in realtime.

        Args:
            log_printer: A callable that processes each message or interruption.
                Defaults to _default_preview_printer which logs messages to the console
                in a human-readable format. You can provide your own function to
                customize how messages are processed.
        """
        for log in self.iter_preview():
            log_printer(log)

    def run(
        self,
        name: str | None = None,
        run_name: str | None = None,
        wait_until_done: bool = False,
    ) -> WorkflowRun:
        """
        Execute the workflow as a batch job.

        This method creates a persistent workflow and runs it as a batch job on
        the Gretel platform. Unlike preview, this creates a permanent record
        of the workflow execution that can be referenced later.

        Args:
            name: Optional name to assign to the workflow. If provided,
                this will override any name previously set with the name() method.
            run_name: Optional name to assign to this specific run of the workflow.
            wait_until_done: Block until the workflow has completed running.
                If set to false the method will immediately return the WorkflowRun
                object.
        Returns:
            WorkflowRun: A WorkflowRun object representing the running workflow.
                This can be used to track the status of the workflow and retrieve
                results when it completes.
        """
        if name:
            self.set_name(name)

        try:
            response = self._data_api.exec_workflow_batch(
                ExecBatchRequest(
                    project_id=self._project_id,
                    workflow_config=self.to_dict(),
                    workflow_id=self._workflow_session_manager.get_id(),
                    workflow_run_name=run_name,
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

        if wait_until_done:
            workflow_run.wait_until_done()

        return workflow_run

    def _ensure_unique_step_name(self, step: Step) -> Step:
        """Ensure the name of the step is unique by appending a counter suffix"""
        ctr = 0
        for step_name in self.step_names:
            if step_name.startswith(step.name):
                ctr += 1
        if ctr > 0:
            step.name = f"{step.name}-{ctr}"
        return step

    def _disambiguate_step_inputs(
        self, step_inputs: list[TaskConfig | Step | str] | None = None
    ) -> list[str] | None:
        disambiguated_step_input_names = []
        for step_input in step_inputs or []:
            if isinstance(step_input, str):
                disambiguated_step_input_names.append(step_input)
                continue
            step_input_hash = _get_object_hash(step_input)
            if step_input_hash in self._step_hash_to_name_map:
                disambiguated_step_input_names.append(
                    self._step_hash_to_name_map[step_input_hash]
                )
        return disambiguated_step_input_names


def _check_for_error_response(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError:
        if 400 <= response.status_code < 500:
            raise NavigatorApiClientError(response.json())
        else:
            raise NavigatorApiServerError(response.json())


def _generate_workflow_name(steps: list[Step] | None = None) -> str:
    """Try and generate a default workflow name.

    If there are steps in the workflow, we try and generate a name
    based on the steps.
    """
    step_str = "my-workflow"
    if steps:
        if len(steps) == 1:
            step_str = f"{steps[0].name}--"
        else:
            step_str = f"{steps[-2].name}--{steps[-1].name}--"

    return f"{step_str}{_generate_random_string()}"


def _generate_random_string(length=6):
    """Generate a random string of letters and numbers."""
    characters = string.ascii_letters + string.digits
    return "".join(random.choice(characters) for _ in range(length)).lower()


def _get_object_hash(obj: Any) -> int:
    """
    Generate a hash of the object provided
    """
    return hash(id(obj))
