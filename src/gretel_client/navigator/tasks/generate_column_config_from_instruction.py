from typing import Optional

from pydantic import BaseModel, Field

from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.generate.generate_column_from_template import (
    GenerateColumnFromTemplateConfig,
)
from gretel_client.navigator.tasks.types import ExistingColumns, ModelSuite


class GenerateColumnConfigFromInstructionConfig(BaseModel):
    name: str
    instruction: str
    edit_task: Optional[GenerateColumnFromTemplateConfig] = None
    existing_columns: ExistingColumns = ExistingColumns()
    must_depend_on: list[str] = Field(default_factory=list)


class GenerateColumnConfigFromInstruction(Task):
    def __init__(
        self,
        name: str,
        instruction: str,
        *,
        edit_task: Optional[GenerateColumnFromTemplateConfig] = None,
        must_depend_on: Optional[list[str]] = None,
        existing_columns: ExistingColumns = ExistingColumns(),
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
        model_suite: ModelSuite = ModelSuite.LLAMA_3_x,  # Defaults to the strongest suite
    ):
        cfg_args = dict(
            name=name,
            instruction=instruction,
            edit_task=edit_task,
            existing_columns=existing_columns,
        )

        if must_depend_on is not None:
            cfg_args = cfg_args | dict(must_depend_on=must_depend_on)

        super().__init__(
            config=GenerateColumnConfigFromInstructionConfig.model_validate(cfg_args),
            workflow_label=workflow_label,
            client=client,
            model_suite=model_suite,
        )

    @property
    def name(self) -> str:
        return "generate_column_config_from_instruction"

    def run(self) -> TaskOutput:
        return self._run()
