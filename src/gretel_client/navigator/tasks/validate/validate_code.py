from enum import Enum
from typing import Optional

from pydantic import BaseModel

from gretel_client.navigator.tasks.base import Task, TaskResults
from gretel_client.navigator.tasks.io import Dataset


class CodeLang(str, Enum):
    PYTHON = "python"
    ANSI_SQL = "sql"
    T_SQL = "t-sql"
    BIGQUERY = "bigquery"
    MY_SQL = "mysql"
    POSTGRE_SQL = "postgresql"


class ValidateCodeConfig(BaseModel):
    code_lang: CodeLang
    validation_columns: list[str] = ["code"]


class ValidateCode(Task):

    def __init__(
        self,
        code_lang: CodeLang,
        validation_columns: list[str] = ["code"],
        workflow_label: Optional[str] = None,
    ):
        super().__init__(
            config=ValidateCodeConfig(
                code_lang=code_lang, validation_columns=validation_columns
            ),
            workflow_label=workflow_label,
        )

    @property
    def name(self) -> str:
        return "validate_code"

    def run(self, dataset: Dataset) -> TaskResults:
        return self._run(dataset=dataset)
