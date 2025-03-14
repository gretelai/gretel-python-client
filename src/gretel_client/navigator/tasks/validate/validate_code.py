from typing import Optional, Union

from pydantic import BaseModel

from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.io import Dataset
from gretel_client.navigator.tasks.types import CodeLang, RecordsT

VALIDATE_PYTHON_COLUMN_SUFFIXES = [
    "_is_valid",
    "_pylint_score",
    "_pylint_severity",
    "_pylint_messages",
]

VALIDATE_SQL_COLUMN_SUFFIXES = [
    "_is_valid",
    "_validator_messages",
]


class ValidateCodeConfig(BaseModel):
    code_lang: CodeLang
    target_columns: list[str]
    result_columns: list[str]


class ValidateCode(Task):

    def __init__(
        self,
        code_lang: CodeLang,
        target_columns: list[str] = ["code"],
        result_columns: list[str] = ["code_is_valid"],
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
    ):
        super().__init__(
            config=ValidateCodeConfig(
                code_lang=code_lang,
                target_columns=target_columns,
                result_columns=result_columns,
            ),
            workflow_label=workflow_label,
            client=client,
        )

    @property
    def name(self) -> str:
        return "validate_code"

    def run(self, dataset: Union[Dataset, RecordsT]) -> TaskOutput:
        return self._run(self._records_to_dataset_if_needed(dataset))
