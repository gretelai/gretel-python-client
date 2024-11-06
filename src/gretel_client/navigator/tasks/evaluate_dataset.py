from typing import Optional, Union

from pydantic import BaseModel, model_validator

from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.io import Dataset
from gretel_client.navigator.tasks.types import RecordsT


class EvaluateDatasetConfig(BaseModel):
    seed_columns: list[str]
    ordered_list_like_columns: list[str] = []
    other_list_like_columns: list[str] = []
    llm_judge_column: Optional[str] = ""
    columns_to_ignore: Optional[list[str]] = []

    @model_validator(mode="before")
    @classmethod
    def check_columns_no_overlap(cls, values: dict) -> dict:
        overlap_list_like_cols = set(
            values.get("ordered_list_like_columns", [])
        ).intersection(set(values.get("other_list_like_columns", [])))
        if overlap_list_like_cols:
            raise ValueError(
                "'ordered_list_like_columns' and 'other_list_like_columns' "
                f"should not have overlap: {overlap_list_like_cols}"
            )
        union_cols = (
            set(values.get("seed_columns", []))
            .union(set(values.get("ordered_list_like_columns", [])))
            .union(set(values.get("other_list_like_columns", [])))
        )
        if values.get("llm_judge_column", "") in union_cols:
            raise ValueError(
                "'llm_judge_column' should not be in 'seed_columns' or "
                "'ordered_list_like_columns' or 'other_list_like_columns'"
            )
        if set(values.get("columns_to_ignore", [])).intersection(union_cols):
            raise ValueError(
                "'columns_to_ignore' should not overlap with 'seed_columns' or "
                "'ordered_list_like_columns' or 'other_list_like_columns'"
            )
        return values


class EvaluateDataset(Task):
    def __init__(
        self,
        seed_columns: list[str],
        ordered_list_like_columns: list[str] = [],
        other_list_like_columns: list[str] = [],
        llm_judge_column: Optional[str] = "",
        columns_to_ignore: Optional[list[str]] = [],
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
    ):
        super().__init__(
            config=EvaluateDatasetConfig(
                seed_columns=seed_columns,
                ordered_list_like_columns=ordered_list_like_columns,
                other_list_like_columns=other_list_like_columns,
                llm_judge_column=llm_judge_column,
                columns_to_ignore=columns_to_ignore,
            ),
            workflow_label=workflow_label,
            client=client,
        )

    @property
    def name(self) -> str:
        return "evaluate_dataset"

    def run(self, dataset: Union[Dataset, RecordsT]) -> TaskOutput:
        return self._run(self._records_to_dataset_if_needed(dataset))
