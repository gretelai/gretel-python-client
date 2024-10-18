import json

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import yaml

from pydantic import BaseModel
from typing_extensions import Self

from gretel_client.navigator.client.utils import get_navigator_client
from gretel_client.navigator.log import get_logger
from gretel_client.navigator.tasks.base import Task

logger = get_logger(__name__, level="DEBUG")

DEFAULT_WORKFLOW_NAME = "navigator-workflow"

TASK_TYPE_EMOJI_MAP = {
    "generate": "🦜",
    "validate": "🔍",
    "sample": "🌱",
    "seed": "🌱",
}


def _get_task_log_emoji(task_name: str) -> str:
    log_emoji = ""
    for task_type, emoji in TASK_TYPE_EMOJI_MAP.items():
        if task_name.startswith(task_type):
            log_emoji = emoji + " "
    return log_emoji


@dataclass
class WorkflowResults:
    dataset: pd.DataFrame
    auxiliary_outputs: Optional[list[dict]] = None


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

    def generate_dataset_preview(self) -> WorkflowResults:
        current_step = None
        auxiliary_outputs = []
        last_step_data_outputs = []

        logger.info("🚀 Generating dataset preview")

        step_idx = 0
        for step_output in self._client.get_workflow_preview(self.to_dict()):
            if not isinstance(step_output, dict):
                step_output = step_output.as_dict()

            logger.debug(f"Step output: {json.dumps(step_output, indent=4)}")

            if step_output["step"] != current_step:
                current_step = step_output["step"]
                # Hacky way to get a decently formatted log output
                task_name = self._steps[step_idx].task.replace("_", "-")
                step_name = step_output["step"].replace("-" + str(step_idx + 1), "")
                label = (
                    ""
                    if task_name == step_name
                    else f" >>{step_name.split(task_name)[-1].replace('-', ' ')}"
                )
                logger.info(
                    f"{_get_task_log_emoji(task_name)}Step {step_idx + 1}: "
                    f"{task_name.replace('-', ' ').capitalize()}{label}"
                )

            if step_output["type"] != "step_state_change":
                step_idx += 1
                if (
                    step_output["step"] == self._steps[-1].name
                    and step_output["type"] == "data_frame"
                ):
                    last_step_data_outputs.append(step_output["output"])
                elif step_output["type"] != "data_frame":
                    auxiliary_outputs.append(step_output["output"])

        df_list = [pd.DataFrame.from_records(r) for r in last_step_data_outputs]
        logger.info("👀 Your preview is ready for a peek!")

        return WorkflowResults(
            dataset=pd.concat(df_list, axis=0),
            auxiliary_outputs=auxiliary_outputs,
        )

    def submit_batch_job(self, num_records: int, project_name: Optional[str] = None):
        return self._client.submit_batch_workflow(
            self.to_dict(), num_records, project_name
        )
