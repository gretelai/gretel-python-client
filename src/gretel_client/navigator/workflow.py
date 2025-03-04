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

from gretel_client.config import ClientConfig
from gretel_client.navigator.client.interface import (
    Client,
    TaskOutput,
    WorkflowInterruption,
)
from gretel_client.navigator.client.remote import Message
from gretel_client.navigator.client.utils import get_navigator_client
from gretel_client.navigator.data_designer.viz_tools import display_sample_record
from gretel_client.navigator.log import get_logger
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.io import Dataset
from gretel_client.navigator.tasks.types import (
    check_model_suite,
    CodeLang,
    DEFAULT_MODEL_SUITE,
    LLMJudgePromptTemplateType,
    ModelSuite,
)
from gretel_client.projects.projects import get_project
from gretel_client.rest_v1.api.workflows_api import WorkflowsApi
from gretel_client.workflows.logs import print_logs_for_workflow_run

logger = get_logger(__name__, level=logging.INFO)

DEFAULT_WORKFLOW_NAME = "navigator-workflow"

TASK_TYPE_EMOJI_MAP = {
    "generate": "🦜",
    "evaluate": "🧐",
    "validate": "🔍",
    "judge": "⚖️",
    "sample": "🎲",
    "seed": "🌱",
    "load": "📥",
    "extract": "💭",
}


def _get_task_log_emoji(task_name: str) -> str:
    log_emoji = ""
    for task_type, emoji in TASK_TYPE_EMOJI_MAP.items():
        if task_name.startswith(task_type):
            log_emoji = emoji + " "
    return log_emoji


def get_task_io_map(client: Client) -> dict:
    """Create a mapping of task names to their inputs and output.

    This is helpful for finding the last step to emit a dataset.
    """
    task_io = {}
    for task in client.registry():
        task_io[task["name"]] = {
            "inputs": task["inputs"],
            "output": task["output"],
        }
    return task_io


def get_last_evaluation_step_name(workflow_step_names: list[str]) -> Optional[str]:
    eval_steps = [s for s in workflow_step_names if s.startswith("evaluate-dataset")]
    return None if len(eval_steps) == 0 else eval_steps[-1]


@dataclass
class DataSpec:
    """Specification for dataset created by DataDesigner.

    We pass this object around to enable streamlined helper methods like
    `display_sample_record`, `fetch_dataset`, and `download_evaluation_report`.
    """

    seed_category_names: list[str]
    data_column_names: list[str]
    seed_subcategory_names: Optional[dict[str, list[str]]] = None
    validation_column_names: Optional[list[str]] = None
    evaluation_column_names: Optional[list[str]] = None
    code_column_names: Optional[list[str]] = None
    code_lang: Optional[CodeLang] = None
    eval_type: Optional[LLMJudgePromptTemplateType] = None
    llm_judge_column_name: Optional[str] = None


@dataclass
class PreviewResults:
    output: Optional[TaskOutput]
    outputs_by_step: dict[str, TaskOutput]
    data_spec: Optional[DataSpec] = None
    evaluation_results: Optional[dict] = None
    _display_cycle_index: int = 0

    @property
    def dataset(self) -> Dataset:
        if isinstance(self.output, pd.DataFrame):
            return self.output

    def display_sample_record(
        self,
        index: Optional[int] = None,
        *,
        syntax_highlighting_theme: str = "dracula",
        background_color: Optional[str] = None,
    ) -> None:
        if self.dataset is None:
            raise ValueError("No dataset found in the preview results.")
        if self.data_spec is None:
            raise ValueError(
                "A data specification is required to display the sample record"
            )
        i = index or self._display_cycle_index
        display_sample_record(
            record=self.dataset.iloc[i],
            seed_categories=self.data_spec.seed_category_names,
            data_columns=self.data_spec.data_column_names,
            seed_subcategories=self.data_spec.seed_subcategory_names,
            background_color=background_color,
            code_columns=self.data_spec.code_column_names,
            validation_columns=self.data_spec.validation_column_names,
            code_lang=self.data_spec.code_lang,
            syntax_highlighting_theme=syntax_highlighting_theme,
            llm_judge_column=self.data_spec.llm_judge_column_name,
            record_index=i,
        )
        if index is None:
            self._display_cycle_index = (self._display_cycle_index + 1) % len(
                self.dataset
            )


TERMINAL_STATUSES = [
    "RUN_STATUS_COMPLETED",
    "RUN_STATUS_ERROR",
    "RUN_STATUS_CANCELLED",
]


class Step(BaseModel):
    name: Optional[str] = None
    task: str
    config: dict
    inputs: Optional[list[str]] = []


class DataDesignerBatchJob:

    def __init__(
        self,
        workflow_run_id: str,
        client: Client,
    ):

        self.workflow_run_id = workflow_run_id

        self._client = client
        self._session = client.client_session
        self._workflow_api = self._session.get_v1_api(WorkflowsApi)
        self._data_spec: Optional[DataSpec] = None

        run = self._get_run()
        self.workflow_id = run.workflow_id
        self.project_id = run.project_id
        self.workflow_step_names = [step.name for step in run.actions]

        self._project = get_project(name=run.project_id, session=self._session)
        self._step_io = {
            step.name: get_task_io_map(self._client)[step.action_type]
            for step in run.actions
        }

    @property
    def last_dataset_step_name(self) -> Optional[str]:
        dataset_steps = [
            s
            for s in self.workflow_step_names
            if self._step_io[s]["output"] == "dataset"
        ]
        return None if len(dataset_steps) == 0 else dataset_steps[-1]

    @property
    def last_evaluation_step_name(self) -> Optional[str]:
        return get_last_evaluation_step_name(self.workflow_step_names)

    @property
    def console_url(self) -> str:
        return (
            f"{self._project.get_console_url().replace(self._project.project_guid, '')}workflows/"
            f"{self.workflow_id}/runs/{self.workflow_run_id}"
        )

    @property
    def workflow_run_status(self) -> str:
        return self._get_run().status

    def _check_if_step_exists(self, step_name: str) -> None:
        if step_name not in self.workflow_step_names:
            raise ValueError(
                f"Step {step_name} not found in workflow."
                f"Available steps: {self.workflow_step_names}"
            )

    def _get_step_output(
        self, step_name: str, output_format: Optional[str] = None
    ) -> TaskOutput:
        return self._client.get_step_output(
            workflow_run_id=self.workflow_run_id,
            step_name=step_name,
            format=output_format,
        )

    def _download_step_output(
        self,
        step_name: str,
        output_format: Optional[str] = None,
        output_dir: Union[str, Path] = ".",
    ) -> Path:
        return self._client.download_step_output(
            workflow_run_id=self.workflow_run_id,
            step_name=step_name,
            output_dir=Path(output_dir),
            format=output_format,
        )

    def _get_run(self):
        return self._workflow_api.get_workflow_run(
            workflow_run_id=self.workflow_run_id, expand=["actions"]
        )

    def _fetch_artifact(
        self,
        step_name: str,
        output_format: str,
        wait_for_completion: bool = False,
        **kwargs,
    ) -> TaskOutput:
        status = self.get_step_status(step_name)
        if status == "RUN_STATUS_COMPLETED":
            if output_format == "parquet":
                logger.info(f"💿 Fetching dataset from workflow step `{step_name}`")
                return self._get_step_output(step_name, output_format=output_format)
            elif output_format == "pdf":
                logger.info(
                    "📊 Downloading evaluation report from completed workflow run"
                )
                output_dir = kwargs.get("output_dir", ".")
                path = self._download_step_output(
                    step_name, output_format="pdf", output_dir=output_dir
                )
                logger.info(f"📄 Evaluation report saved to {path}")
                return path
            elif output_format == "json":
                logger.info(f"📦 Fetching output from step `{step_name}`")
                return self._get_step_output(step_name, output_format=output_format)
            else:
                raise ValueError(f"Unknown output type: {output_format}")
        elif status in {"RUN_STATUS_ERROR", "RUN_STATUS_LOST"}:
            logger.error("🛑 Workflow run failed. Cannot fetch step output.")
        elif status in {"RUN_STATUS_CANCELLING", "RUN_STATUS_CANCELLED"}:
            logger.warning("⚠️ Workflow run was cancelled.")
        elif status in {
            "RUN_STATUS_PENDING",
            "RUN_STATUS_CREATED",
            "RUN_STATUS_ACTIVE",
            "RUN_STATUS_UNKNOWN",
        }:
            if wait_for_completion:
                logger.info(
                    f"⏳ Waiting for workflow step `{step_name}` to complete..."
                )
                self.wait_for_completion(step_name)
                return self._fetch_artifact(step_name, output_format, **kwargs)
            else:
                logger.warning(
                    f"🏗️ We are still building the requested artifact from step '{step_name}'. "
                    "Set `wait_for_completion=True` to wait for the step to complete. "
                    f"Workflow status: {self.workflow_run_status}."
                )
        else:
            logger.error(f"Unknown step status: {status}")

    def _reached_terminal_status(self, step_name: Optional[str] = None) -> bool:
        status = (
            self.workflow_run_status
            if step_name is None
            else self.get_step_status(step_name)
        )
        return status in TERMINAL_STATUSES

    def get_step_status(self, step_name: str) -> str:
        self._check_if_step_exists(step_name)
        run = self._get_run()
        return [a for a in run.actions if a.name == step_name][0].status

    def poll_logs(self) -> None:
        print_logs_for_workflow_run(self.workflow_run_id, self._session)

    def wait_for_completion(self, step_name: Optional[str] = None) -> None:
        self._check_if_step_exists(step_name)
        logger.info(f"👀 Follow along -> {self.console_url}")
        while True:
            if self._reached_terminal_status(step_name):
                break
            time.sleep(10)

    def fetch_step_output(
        self,
        step_name: str,
        *,
        output_format: Optional[str] = None,
        wait_for_completion: bool = False,
        **kwargs,
    ) -> TaskOutput:
        self._check_if_step_exists(step_name)
        if output_format is None:
            output_format = (
                "parquet" if self._step_io[step_name]["output"] == "dataset" else "json"
            )
        return self._fetch_artifact(
            step_name,
            output_format=output_format,
            wait_for_completion=wait_for_completion,
            **kwargs,
        )

    def fetch_dataset(self, *, wait_for_completion: bool = False) -> Dataset:
        if self.last_dataset_step_name is None:
            raise ValueError("The Workflow did not contain a dataset.")
        return self._fetch_artifact(
            step_name=self.last_dataset_step_name,
            output_format="parquet",
            wait_for_completion=wait_for_completion,
        )

    def download_evaluation_report(
        self,
        *,
        wait_for_completion: bool = False,
        output_dir: Union[str, Path] = Path("."),
    ) -> None:
        if self.last_evaluation_step_name is None:
            raise ValueError("The Workflow did not contain an evaluation step.")
        return self._fetch_artifact(
            step_name=self.last_evaluation_step_name,
            output_format="pdf",
            wait_for_completion=wait_for_completion,
            output_dir=output_dir,
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(\n"
            f"    workflow_run_id: {self.workflow_run_id}\n"
            f"    workflow_run_status: {self.workflow_run_status}\n"
            f"    console_url: {self.console_url}\n"
            ")"
        )


class DataDesignerWorkflow:

    @staticmethod
    def create_steps_from_sequential_tasks(
        task_list: list[Task], *, verbose_logging: bool = False
    ) -> list[Step]:
        steps = []
        step_names = []
        if verbose_logging:
            logger.info("⚙️ Configuring Data Designer Workflow steps:")
        for i in range(len(task_list)):
            inputs = []
            task = task_list[i]
            suffix = "" if task.workflow_label is None else f"-{task.workflow_label}"
            name = f"{task.name}-{i + 1}{suffix}".replace("_", "-").replace(" ", "-")
            if verbose_logging:
                logger.info(f"  |-- Step {i + 1}: {name}")
            step_names.append(name)
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
        cls,
        client: Client,
        task_list: list[Task],
        *,
        workflow_name: str = None,
    ) -> Self:
        workflow = cls(client=client, workflow_name=workflow_name)
        workflow.add_steps(cls.create_steps_from_sequential_tasks(task_list))
        return workflow

    @classmethod
    def from_yaml(cls, client: Client, yaml_str: str) -> Self:
        yaml_dict: dict = yaml.safe_load(yaml_str)
        workflow = cls(client=client, workflow_name=yaml_dict["name"])
        workflow.add_steps([Step(**step) for step in yaml_dict["steps"]])
        workflow.set_globals(yaml_dict.get("globals", None))
        return workflow

    def set_globals(self, new_globals: dict):
        self._globals.update(new_globals)

    def __init__(
        self,
        client: Client,
        *,
        steps: Optional[list[Step]] = None,
        model_suite: ModelSuite = DEFAULT_MODEL_SUITE,
        workflow_name: Optional[str] = None,
        fetch_task_io: bool = True,
    ):
        self._workflow_name = (
            workflow_name
            or f"{DEFAULT_WORKFLOW_NAME}-{datetime.now().isoformat(timespec='seconds')}"
        )
        self._steps = steps or []
        self._client = client
        self._model_suite = check_model_suite(model_suite)
        self._globals = {
            "num_records": 10,
            "model_suite": self._model_suite,
        }
        self._task_io = {}
        if fetch_task_io:
            self._task_io = get_task_io_map(self._client)

        # we track the workflow and project id to ensure that we can tie
        # multiple batch workflow calls within a session to the same workflow
        self.workflow_id = None
        self.project_id = None

    @property
    def steps(self) -> list[Step]:
        return self._steps

    @property
    def workflow_step_names(self) -> list[str]:
        return [s.name or "" for s in self._steps]

    @property
    def step_io_map(self) -> dict[str, dict[str, str]]:
        return {s.name: self._task_io[s.task] for s in self._steps}

    def generate_preview(self, verbose_logging: bool = False) -> PreviewResults:
        step_idx = 0
        message: Message
        current_step = None
        final_output = None
        outputs_by_step = {}
        for message in self._client.get_workflow_preview(self.to_dict()):
            if isinstance(message, WorkflowInterruption):
                logger.warning(message.message)
                break
            if not message.step:
                continue
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
                if (level == "info" and verbose_logging) or level in [
                    "error",
                    "warning",
                ]:
                    logger.info(f"  {'|' if '|--' in msg else '|--'} {msg}")

            if message.stream == "step_outputs":
                logger.debug(f"Step output: {json.dumps(message.payload, indent=4)}")

                output = message.payload
                if message.type == "dataset":
                    output = pd.DataFrame.from_records(message.payload.get("dataset"))
                    final_output = output
                outputs_by_step[message.step] = output
        # the final output is either the dataset produced by the last
        # task in the workflow, or, if no dataset is produced by the workflow
        # the final output will be the output of the last task to complete
        # (which may also be none)
        last_evaluation_step_name = get_last_evaluation_step_name(
            workflow_step_names=self.workflow_step_names
        )
        if final_output is None:
            final_output = outputs_by_step.get(current_step)
        evaluation_results = (
            None
            if last_evaluation_step_name is None
            else outputs_by_step.get(last_evaluation_step_name)
        )
        preview_results = PreviewResults(
            output=final_output,
            outputs_by_step=outputs_by_step,
            evaluation_results=evaluation_results,
        )
        return preview_results

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
            globals=self._globals or {},
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

    def submit_batch_job(
        self,
        num_records: int,
        *,
        project_name: Optional[str] = None,
        workflow_id: Optional[str] = None,
    ) -> DataDesignerBatchJob:
        self._globals.update({"num_records": num_records})

        # multiple calls to submit_batch_workflow within a session
        # should produce workflow runs that are a part of the same
        # project and workflow.
        response = self._client.submit_batch_workflow(
            self.to_dict(),
            num_records,
            project_name or self.project_id,
            workflow_id or self.workflow_id,
        )

        self.workflow_id = response.workflow_id
        self.project_id = response.project.name

        return DataDesignerBatchJob(
            workflow_run_id=response.workflow_run_id, client=self._client
        )
