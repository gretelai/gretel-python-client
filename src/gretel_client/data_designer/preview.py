from dataclasses import dataclass

import pandas as pd

from gretel_client.data_designer.types import DataPipelineMetadata, TaskOutputT
from gretel_client.data_designer.viz_tools import display_sample_record
from gretel_client.workflows.io import Dataset


@dataclass
class PreviewResults:
    data_pipeline_metadata: DataPipelineMetadata
    output: TaskOutputT | None = None
    evaluation_results: dict | None = None
    _display_cycle_index: int = 0

    @property
    def dataset(self) -> Dataset | None:
        if isinstance(self.output, pd.DataFrame):
            return Dataset(self.output)
        return None

    def display_sample_record(
        self,
        index: int | None = None,
        *,
        syntax_highlighting_theme: str = "dracula",
        background_color: str | None = None,
    ) -> None:
        if self.dataset is None:
            raise ValueError("No dataset found in the preview results.")
        i = index or self._display_cycle_index
        dp_metadata = self.data_pipeline_metadata
        display_sample_record(
            record=self.dataset.df.iloc[i],
            sampling_based_column_names=dp_metadata.sampling_based_column_names,
            prompt_based_column_names=dp_metadata.prompt_based_column_names,
            code_lang=dp_metadata.code_lang,
            code_columns=dp_metadata.code_column_names,
            validation_columns=dp_metadata.validation_column_names,
            llm_judge_column=dp_metadata.llm_judge_column_name,
            background_color=background_color,
            syntax_highlighting_theme=syntax_highlighting_theme,
            record_index=i,
        )
        if index is None:
            self._display_cycle_index = (self._display_cycle_index + 1) % len(
                self.dataset.df
            )
