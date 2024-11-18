"""
Base class and common utils for fine-tuning models.
"""

from __future__ import annotations

import datetime
import uuid

from pathlib import Path
from typing import Optional, TYPE_CHECKING, Union

import pandas as pd

from pydantic import BaseModel, Field, model_validator

if TYPE_CHECKING:
    from gretel_client.fine_tuning.formatters import BaseFormatter


class OpenAIFile(BaseModel):
    id: str
    status: str
    open_ai_data: Optional[dict] = None


class OpenAIFineTuneJob(BaseModel):
    id: str
    model: str
    status: str
    open_ai_data: Optional[dict] = Field(default_factory=dict)


class FineTuningEvent(BaseModel):
    id: str
    created_at: int
    created_at_str: str = Field(default_factory=lambda: "")
    message: str
    object: str
    open_ai_data: Optional[dict] = None

    @model_validator(mode="before")
    @classmethod
    def set_created_at_str(cls, values):
        if (created_at := values.get("created_at")) is not None:
            values["created_at_str"] = datetime.datetime.fromtimestamp(
                created_at, tz=datetime.timezone.utc
            ).isoformat()
        return values


class FineTuningCheckpoint(BaseModel):
    id: str = Field(default_factory=lambda: f"gretel-{str(uuid.uuid4())}")
    open_ai_training_file: Optional[OpenAIFile] = None
    open_ai_validation_file: Optional[OpenAIFile] = None
    open_ai_fine_tune_job: Optional[OpenAIFineTuneJob] = None
    open_ai_fine_tune_job_events: list[FineTuningEvent] = []

    @property
    def open_ai_last_fine_tune_event_id(self) -> Optional[str]:
        if not self.open_ai_fine_tune_job_events:
            return None
        return self.open_ai_fine_tune_job_events[-1].id

    @property
    def open_ai_fine_tuned_model_id(self) -> Optional[str]:
        if (
            self.open_ai_fine_tune_job is None
            or self.open_ai_fine_tune_job.open_ai_data is None
        ):
            return None
        return self.open_ai_fine_tune_job.open_ai_data.get("fine_tuned_model")

    def reset_open_ai_fine_tune_job(self) -> None:
        self.open_ai_fine_tune_job = None
        self.open_ai_fine_tune_job_events = []

    def reset_open_ai_training_file(self) -> None:
        self.open_ai_training_file = None

    def reset_open_ai_validation_file(self) -> None:
        self.open_ai_validation_file = None

    def reset_files(self) -> None:
        self.reset_open_ai_training_file()
        self.reset_open_ai_validation_file()


class BaseFineTuner:

    formatter: Optional[BaseFormatter]
    """
    Required unless restoring from a checkpoint.
    """
    checkpoint: FineTuningCheckpoint
    train_data: Optional[pd.DataFrame]
    """
    Required unleses restoring from a checkpoint.
    """
    validation_data: Optional[pd.DataFrame]

    def __init__(
        self,
        *,
        train_data: Optional[pd.DataFrame] = None,
        validation_data: Optional[pd.DataFrame] = None,
        formatter: Optional[BaseFormatter] = None,
        checkpoint: Optional[Union[FineTuningCheckpoint, str, dict]] = None,
    ):
        self.train_data = train_data
        self.validation_data = validation_data
        self.formatter = formatter
        if checkpoint is None:
            self.checkpoint = FineTuningCheckpoint()
            if self.train_data is None or self.formatter is None:
                raise ValueError(
                    "train_data and formatter are required if checkpoint is not provided"
                )
        else:
            if isinstance(checkpoint, dict):
                checkpoint = FineTuningCheckpoint.model_validate(checkpoint)
            elif isinstance(checkpoint, str):
                path = Path(checkpoint)
                if not path.exists():
                    raise FileNotFoundError(f"Checkpoint file not found: {path}")
                checkpoint = FineTuningCheckpoint.model_validate_json(path.read_text())
            self.checkpoint = checkpoint

    def save_checkpoint(self, path: Union[str, Path]) -> None:
        path = Path(path)
        path.write_text(self.checkpoint.model_dump_json(indent=2))

    def load_checkpoint(self, path: Union[str, Path]) -> None:
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Checkpoint file not found: {path}")
        self.checkpoint = FineTuningCheckpoint.model_validate_json(path.read_text())
