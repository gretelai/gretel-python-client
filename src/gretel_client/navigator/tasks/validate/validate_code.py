from typing import Optional

from pydantic import BaseModel

from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.io import Dataset
from gretel_client.navigator.tasks.types import CodeLang


class ValidateCodeConfig(BaseModel):
    code_lang: CodeLang
    code_columns: list[str] = ["code"]


class ValidateCode(Task):

    def __init__(
        self,
        code_lang: CodeLang,
        code_columns: list[str] = ["code"],
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
    ):
        super().__init__(
            config=ValidateCodeConfig(code_lang=code_lang, code_columns=code_columns),
            workflow_label=workflow_label,
            client=client,
        )

    @property
    def name(self) -> str:
        return "validate_code"

    def run(self, dataset: Dataset) -> TaskOutput:
        return self._run(dataset)
