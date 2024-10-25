import json
import logging
import time

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import yaml

from pydantic import BaseModel
from typing_extensions import Self

from gretel_client.analysis_utils import display_dataframe_in_notebook
from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.client.remote import Message
from gretel_client.navigator.client.utils import get_navigator_client
from gretel_client.navigator.log import get_logger
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.io import Dataset
from gretel_client.projects import Project
from gretel_client.rest_v1.api.workflows_api import WorkflowsApi
from gretel_client.rest_v1.api_client import ApiClient
from gretel_client.workflows.logs import print_logs_for_workflow_run

logger = get_logger(__name__, level=logging.INFO)

DEFAULT_WORKFLOW_NAME = "navigator-workflow"

TASK_TYPE_EMOJI_MAP = {
    "generate": "🦜",
    "validate": "🔍",
    "sample": "🌱",
    "seed": "🌱",
    "load": "📥",
}


def _get_task_log_emoji(task_name: str) -> str:
    log_emoji = ""
    for task_type, emoji in TASK_TYPE_EMOJI_MAP.items():
        if task_name.startswith(task_type):
            log_emoji = emoji + " "
    return log_emoji


@dataclass
class WorkflowResults:
    output: Dataset
    outputs_by_step: dict[str, TaskOutput]

    def display_dataframe_in_notebook(
        self, num_records: int = 10, settings: Optional[dict] = None
    ) -> None:
        """Display preview as pandas DataFrame in notebook with better settings for readability.

        This function is intended to be used in a Jupyter notebook.

        Args:
            num_records: The number of records to display.
            settings: Optional properties to set on the DataFrame's style.
                If None, default settings with text wrapping are used.
        """
        display_dataframe_in_notebook(self.output.head(num_records), settings=settings)


_TERMINAL_STATUSES = [
    "RUN_STATUS_COMPLETED",
    "RUN_STATUS_ERROR",
    "RUN_STATUS_CANCELLED",
]


class BatchWorkflowRun:
    workflow_id: str
    workflow_run_id: str
    _client: Client
    _project: Project
    _workflow_api: ApiClient

    def __init__(
        self, project: Project, client: Client, workflow_id: str, workflow_run_id: str
    ):
        self.workflow_id = workflow_id
        self.workflow_run_id = workflow_run_id
        self._client = client
        self._project = project
        self._workflow_api = project.session.get_v1_api(WorkflowsApi)

    @property
    def console_url(self) -> str:
        return (
            f"{self.project.get_console_url().replace(self.project.project_guid, '')}workflows/"
            f"{self.workflow_id}/runs/{self.workflow_run_id}"
        )

    def wait_for_completion(self) -> None:
        while True:
            if self._reached_terminal_status():
                break
            time.sleep(15)

    def run_status(self) -> str:
        run = self._workflow_api.get_workflow_run(workflow_run_id=self.workflow_run_id)
        return run.status

    def _reached_terminal_status(self) -> bool:
        status = self.run_status()
        return status in _TERMINAL_STATUSES

    def poll_logs(self) -> None:
        print_logs_for_workflow_run(self.workflow_run_id, self._project.session)

    def get_step_output(
        self, step_name: str, format: Optional[str] = None
    ) -> TaskOutput:
        return self._client.get_step_output(
            workflow_run_id=self.workflow_run_id,
            step_name=step_name,
            format=format,
        )

    def download_step_output(
        self,
        step_name: str,
        format: Optional[str] = None,
        output_dir: Union[str, Path] = ".",
    ) -> Path:
        return self._client.download_step_output(
            workflow_run_id=self.workflow_run_id,
            step_name=step_name,
            output_dir=Path(output_dir),
            format=format,
        )


class Step(BaseModel):
    name: Optional[str] = None
    task: str
    config: dict
    inputs: Optional[list[str]] = []


class NavigatorWorkflow:
    def __init__(
        self,
        steps: Optional[list[Step]] = None,
        workflow_name: Optional[str] = None,
        **session_kwargs,
    ):
        self._workflow_name = (
            workflow_name
            or f"{DEFAULT_WORKFLOW_NAME}-{datetime.now().isoformat(timespec='seconds')}"
        )
        self._client = get_navigator_client(**session_kwargs)
        self._steps = steps or []

    @staticmethod
    def create_steps_from_sequential_tasks(task_list: list[Task]) -> list[Step]:
        steps = []
        step_names = []
        for i in range(len(task_list)):
            inputs = []
            task = task_list[i]
            suffix = "" if task.workflow_label is None else f"-{task.workflow_label}"
            step_names.append(
                f"{task.name}-{i + 1}{suffix}".replace("_", "-").replace(" ", "-")
            )
            if i > 0:
                prev_name = step_names[i - 1]
                inputs = [prev_name]
            steps.append(
                Step(
                    name=step_names[i],
                    task=task.name,
                    config=task.config.model_dump(),
                    inputs=inputs,
                )
            )
        return steps

    @classmethod
    def from_sequential_tasks(
        cls, task_list: list[Task], workflow_name: str = None, **session_kwargs
    ) -> Self:
        workflow = cls(workflow_name=workflow_name, **session_kwargs)
        workflow.add_steps(cls.create_steps_from_sequential_tasks(task_list))
        return workflow

    @classmethod
    def from_yaml(cls, yaml_str: str) -> Self:
        yaml_dict = yaml.safe_load(yaml_str)
        workflow = cls(workflow_name=yaml_dict["name"])
        workflow.add_steps([Step(**step) for step in yaml_dict["steps"]])
        return workflow

    def add_step(self, step: Step) -> None:
        self._steps.append(step)

    def add_steps(self, steps: list[Step]) -> None:
        self._steps.extend(steps)

    def reset_steps(self) -> None:
        self._steps = []

    def to_dict(self) -> dict:
        return dict(
            name=self._workflow_name,
            steps=list(
                map(lambda x: x.model_dump() if isinstance(x, Step) else x, self._steps)
            ),
        )

    def to_json(self, file_path: Optional[Union[Path, str]] = None) -> Optional[str]:
        json_str = json.dumps(self.to_dict(), indent=4)
        if file_path is None:
            return json_str
        with open(file_path, "w") as f:
            f.write(json_str)

    def to_yaml(self, file_path: Optional[Union[Path, str]] = None) -> Optional[str]:
        yaml_str = yaml.dump(json.loads(self.to_json()), default_flow_style=False)
        if file_path is None:
            return yaml_str
        with open(file_path, "w") as f:
            f.write(yaml_str)

    def generate_dataset_preview(
        self, *, verbose_logging: bool = False
    ) -> WorkflowResults:
        current_step = None
        final_output = None
        outputs_by_step = {}

        logger.info("🚀 Generating dataset preview")

        step_idx = 0
        message: Message
        for message in self._client.get_workflow_preview(self.to_dict()):
            if current_step != message.step:
                current_step = message.step
                task_name = self._steps[step_idx].task.replace("_", "-")
                step_name = message.step.replace("-" + str(step_idx + 1), "")
                label = (
                    ""
                    if task_name == step_name
                    else f" >>{step_name.split(task_name)[-1].replace('-', ' ')}"
                )
                logger.info(
                    f"{_get_task_log_emoji(task_name)}Step {step_idx + 1}: "
                    f"{task_name.replace('-', ' ').capitalize()}{label}"
                )
                step_idx += 1

            # todo: make this log level aware
            if message.stream == "logs":
                level, msg = message.payload.get("level"), message.payload.get("msg")
                if level is not None and verbose_logging:
                    logger.info(f"    |-- {msg}")

            if message.stream == "step_outputs":
                logger.debug(f"Step output: {json.dumps(message.payload, indent=4)}")

                output = message.payload
                if message.type == "dataset":
                    output = pd.DataFrame.from_records(message.payload.get("dataset"))
                    final_output = output
                outputs_by_step[message.step] = output

        logger.info("👀 Your dataset preview is ready for a peek!")

        # the final output is either the dataset produced by the last
        # task in the workflow, or, if no dataset is produced by the workflow
        # the final output will be the output of the last task.
        if final_output is None:
            final_output = outputs_by_step[current_step]

        return WorkflowResults(output=final_output, outputs_by_step=outputs_by_step)

    def submit_batch_job(self, num_records: int, project_name: Optional[str] = None):
        response = self._client.submit_batch_workflow(
            self.to_dict(), num_records, project_name
        )
        return BatchWorkflowRun(
            workflow_id=response.workflow_id,
            workflow_run_id=response.workflow_run_id,
            client=self._client,
            project=response.project,
        )
