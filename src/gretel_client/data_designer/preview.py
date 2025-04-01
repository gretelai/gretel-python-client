from dataclasses import dataclass

import pandas as pd

from gretel_client.data_designer.types import TaskOutputT
from gretel_client.data_designer.viz_tools import AIDDMetadata, display_sample_record
from gretel_client.workflows.io import Dataset


@dataclass
class PreviewResults:
    aidd_metadata: AIDDMetadata
    output: TaskOutputT | None = None
    evaluation_results: dict | None = None
    success: bool = True
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
        hide_seed_columns: bool = False,
        syntax_highlighting_theme: str = "dracula",
        background_color: str | None = None,
    ) -> None:
        if self.dataset is None:
            raise ValueError("No dataset found in the preview results.")
        i = index or self._display_cycle_index
        display_sample_record(
            record=self.dataset.df.iloc[i],
            aidd_metadata=self.aidd_metadata,
            background_color=background_color,
            syntax_highlighting_theme=syntax_highlighting_theme,
            hide_seed_columns=hide_seed_columns,
            record_index=i,
        )
        if index is None:
            self._display_cycle_index = (self._display_cycle_index + 1) % len(
                self.dataset.df
            )
