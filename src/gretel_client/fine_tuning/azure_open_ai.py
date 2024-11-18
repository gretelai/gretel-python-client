"""
Fine Tuning Support for the Azure OpenAI Service.

This module wraps the OpenAI client and provides a high-level interface for creating and managing fine-tuning jobs.
"""

from __future__ import annotations

import json
import logging
import re
import sys
import tempfile
import time

from pathlib import Path
from typing import Optional, TYPE_CHECKING, Union

import pandas as pd
import tqdm

from openai.types.fine_tuning.job_create_params import Hyperparameters

from gretel_client.fine_tuning.base import (
    BaseFineTuner,
    FineTuningEvent,
    OpenAIFile,
    OpenAIFineTuneJob,
)
from gretel_client.fine_tuning.formatters import OpenAIFormatter

if TYPE_CHECKING:
    from openai import AzureOpenAI

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


_JSON_PATTERN = r"^\s*```json|```\s*$"


def _clean_json_string(json_str: str) -> str:
    return re.sub(_JSON_PATTERN, "", json_str.strip())


class OpenAIFineTuner(BaseFineTuner):
    """
    Create and manage fine-tuning jobs with the Azure OpenAI service. This instance will
    track the underlying metadata for a single fine-tuning job, including the training and
    validation data, and fine-tuning event logs.

    Args:
        openai_client: An instance of the Azure OpenAI client.
        formatter: An instance of a data formatter that will be used to prepare the training and (optional)
            validation data. This formatter is required to be set before any data can be uploaded as it is
            used to format the training data into the OpenAI fine-tuning format. If you are only restoring
            from a checkpoint file and training and validation data is already uploaded, you do not need to
            provide a formatter instance.
        train_data: The training data to be used for fine-tuning. This data will be formatted using the
            provided `formatter` and uploaded to the OpenAI service.
        validation_data: The validation data to be used for fine-tuning. This data will be formatted using
            the provided `formatter` and uploaded to the OpenAI service.
        checkpoint: The path for a checkpoint file that can be used to restore the metadata for a previous
            fine-tuning instance.
    """

    _client: AzureOpenAI
    formatter: Optional[OpenAIFormatter]  # type: ignore

    def __init__(
        self,
        *,
        openai_client: AzureOpenAI,
        formatter: Optional[OpenAIFormatter] = None,
        **kwargs,
    ):
        super().__init__(formatter=formatter, **kwargs)
        self._client = openai_client

    def prepare_and_upload_data(self, wait: bool = True) -> None:
        """
        Format and upload the training and validation data (if provided) to the OpenAI service as Files.
        This method will use the provided `formatter` to create the fine-tuning datasets and automatically
        upload them to the fine-tuning service.

        If the checkpoint already contains an OpenAI file ID, this method will skip the upload process
        for that particular file.

        Args:
            wait: When True, the method will wait for the upload(s) to be complete. If Wait is False,
                the method will return immediately after the upload request is made. You can use the
                `wait_for_files` method to wait for the files to be ready.

        Raises:
            ValueError: If the training data or formatter is not set on the instance.
        """
        if self.checkpoint.open_ai_training_file is not None:
            logger.info("✅ Training data already uploaded, skipping.")
        else:
            if self.train_data is None or self.formatter is None:
                raise ValueError(
                    "Training data and a training data formatter must have been provided at instance creation."
                )
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8-sig", prefix="train-", suffix=".jsonl"
            ) as tmp_file:
                logger.info("🏗️ Creating fine-tuning dataset.")
                self.formatter.create_ft_dataset(
                    input_dataset=self.train_data, output_file=tmp_file.name
                )
                logger.info(
                    f"📤 Uploading training dataset: {Path(tmp_file.name).stem}"
                )
                training_response = self._client.files.create(
                    file=open(tmp_file.name, "rb"), purpose="fine-tune"
                )
                self.checkpoint.open_ai_training_file = OpenAIFile(
                    id=training_response.id, status=training_response.status
                )

        if self.checkpoint.open_ai_validation_file is not None:
            logger.info("✅ Validation data already uploaded, skipping.")
        elif self.validation_data is not None:
            if self.formatter is None:
                raise ValueError(
                    "Validation data and a data formatter must have been provider at instance creation."
                )
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8-sig", prefix="validation-", suffix=".jsonl"
            ) as tmp_file:
                logger.info("🏗️ Creating validation dataset.")
                self.formatter.create_ft_dataset(
                    input_dataset=self.validation_data,
                    output_file=tmp_file.name,
                )
                logger.info(
                    f"📤 Uploading validation dataset: {Path(tmp_file.name).stem}"
                )
                validation_response = self._client.files.create(
                    file=open(tmp_file.name, "rb"), purpose="fine-tune"
                )
                self.checkpoint.open_ai_validation_file = OpenAIFile(
                    id=validation_response.id, status=validation_response.status
                )

        if wait:
            self.wait_for_files()

    def update_file_status(self, verbose: bool = True) -> bool:
        """
        Retrieve the status of the training and validation (if applicable) files.

        Args:
            verbose: If True, extra logging will be output.

        Returns:
            True if both files are ready, False otherwise.

        Raises:
            RuntimeError: If the file(s) failed to process.
        """
        training_ready = False
        validation_ready = False

        if self.checkpoint.open_ai_training_file is not None:
            response = self._client.files.retrieve(
                self.checkpoint.open_ai_training_file.id
            )
            self.checkpoint.open_ai_training_file.status = response.status
            self.checkpoint.open_ai_training_file.open_ai_data = response.model_dump()
            if self.checkpoint.open_ai_training_file.status == "error":
                raise RuntimeError(
                    f"Training file failed to process, received error: {response.status_details}"
                )
            if verbose:
                logger.info(
                    f"💡 Training file status: {self.checkpoint.open_ai_training_file.status}"
                )
            if self.checkpoint.open_ai_training_file.status == "processed":
                training_ready = True
        else:
            if verbose:
                logger.info("🤨 No training file to update status for.")

        if self.checkpoint.open_ai_validation_file is not None:
            response = self._client.files.retrieve(
                self.checkpoint.open_ai_validation_file.id
            )
            self.checkpoint.open_ai_validation_file.status = response.status
            self.checkpoint.open_ai_validation_file.open_ai_data = response.model_dump()
            if self.checkpoint.open_ai_validation_file.status == "error":
                raise RuntimeError(
                    f"Validation file failed to process, received error: {response.status_details}"
                )
            if verbose:
                logger.info(
                    f"💡 Validation file status: {self.checkpoint.open_ai_validation_file.status}"
                )
            if self.checkpoint.open_ai_validation_file.status == "processed":
                validation_ready = True
        else:
            if verbose:
                logger.info("❎ No validation file to update status for.")

            # It's OK to not have validation data, so we can flip the switch here as
            # we'll only care about training data being ready.
            validation_ready = True

        return all([training_ready, validation_ready])

    def wait_for_files(self, timeout_seconds: int = 120) -> None:
        """
        Wait for uploaded file(s) to be ready for use in fine-tuning.

        Args:
            timeout_seconds: The maximum number of seconds to wait for the files to be ready. If this timeout
                is exceeded a TimeoutError will be raised.

        Raises:
            TimeoutError: If the file(s) are not ready within the specified time.
            RuntimeError: If the file(s) failed to process (this is raised by the `update_file_status` method).
        """
        if self.checkpoint.open_ai_training_file is None:
            logger.info("🤔 No files to wait for.")
            return

        # Check if the files are already ready, if so, no need to get into
        # our loop
        if self.update_file_status():
            logger.info("✅ Files are ready.")
            return

        if self.checkpoint.open_ai_validation_file is not None:
            logger.info("🕒 Waiting for training and validation files to be ready.")
        else:
            logger.info("🕒 Waiting for training file to be ready.")

        stop_at = time.time() + timeout_seconds
        while time.time() < stop_at:
            if self.update_file_status(verbose=False):
                logger.info("✅ Files are ready.")
                return
            time.sleep(5)

        raise TimeoutError("Timed out waiting for files to be ready.")

    def start_fine_tuning(
        self,
        model: str,
        *,
        epochs: Optional[int] = None,
        batch_size: Optional[int] = None,
        learning_rate_multiplier: Optional[int] = None,
        wait: bool = True,
        force: bool = False,
        checkpoint_save_path: Optional[str] = None,
    ) -> None:
        """
        Start a fine-tuning job on the Azure OpenAI service.

        Args:
            model: The name of the model to fine-tune.
            epochs: The number of epochs to train for.
            batch_size: The batch size to use during training.
            learning_rate_multiplier: A multiplier to apply to the learning rate.
            wait: When True, the method will wait for the fine-tuning job to complete before returning.
            force: When True, any existing fine-tuning job will be reset before starting a new job.
            checkpoint_save_path: If set, the checkpoint will be saved to this path routinely during the fine-tuning job.
                This only applies if `wait` is True.

        Raises:
            RuntimeError: If the fine-tuning job has already been created or if training data has not been uploaded.
        """
        if force:
            self.checkpoint.reset_open_ai_fine_tune_job()

        if self.checkpoint.open_ai_fine_tune_job is not None:
            raise RuntimeError(
                f"Fine-tuning job already created: {self.checkpoint.open_ai_fine_tune_job.id}"
            )
        if self.checkpoint.open_ai_training_file is None:
            raise RuntimeError("Training data must be uploaded before fine-tuning.")

        params = Hyperparameters(
            batch_size=batch_size,  # type: ignore
            n_epochs=epochs,  # type: ignore
            learning_rate_multiplier=learning_rate_multiplier,  # type: ignore
        )

        logger.info("🚀 Starting fine-tuning job.")
        job = self._client.fine_tuning.jobs.create(
            model=model,
            training_file=self.checkpoint.open_ai_training_file.id,
            hyperparameters=params,
            validation_file=(
                self.checkpoint.open_ai_validation_file.id
                if self.checkpoint.open_ai_validation_file
                else None
            ),
        )
        self.checkpoint.open_ai_fine_tune_job = OpenAIFineTuneJob(
            model=model, id=job.id, status=job.status, open_ai_data=job.model_dump()
        )
        logger.info(f"📈 Fine-tuning job created: {job.id}")
        if wait:
            self.wait_for_fine_tune_job(checkpoint_save_path=checkpoint_save_path)

    def _read_until_last_checkpoint(self) -> list[dict]:
        buffer = []
        after = None

        read_until_beginning = True

        # NOTE: The event list can change under you, where the events start
        # showing every 10 steps vs every step, this makes it hard to keep
        # track of the actual last event ID we've seen since that event ID
        # could actually not appear in the list of events.
        # Will need to figure out how to handle this but for now we'll just
        # pull all events every time.

        # if self.checkpoint.open_ai_last_fine_tune_event_id is not None:
        #    read_until_beginning = False

        while True:
            events = self._client.fine_tuning.jobs.list_events(
                self.checkpoint.open_ai_fine_tune_job.id,  # type: ignore - we don't use this function w/o a FT job id
                after=after,  # type: ignore
                limit=500,
            )

            events_dict = events.model_dump()
            if events_dict.get("object") != "list":
                logger.warning(
                    "🤔 Fine-tuning job events are not in the expected format."
                )
                continue

            if read_until_beginning:
                buffer.extend(events_dict["data"])
                if events_dict.get("has_more") is False:
                    break
                elif events_dict["data"]:
                    after = events_dict["data"][-1]["id"]
            else:
                if not events_dict["data"] and events_dict.get("has_more") is False:
                    break
                for event_dict in events_dict["data"]:
                    if (
                        event_dict["id"]
                        == self.checkpoint.open_ai_last_fine_tune_event_id
                    ):
                        return buffer
                    buffer.append(event_dict)
                    after = event_dict["id"]
        return buffer

    def update_fine_tune_job_status(self, verbose: bool = True) -> bool:
        """
        Retrieve the status of the current fine-tuning job from the checkpoint.
        This method retrieves the fine-tuning event list on each call and stores the events
        on the checkpoint.

        Args:
            verbose: If True, logs will be output for the fine-tuning job status and events.

        Returns:
            bool: True if the fine-tuning job has completed successfully, False otherwise.

        Raises:
            ValueError: If there is no fine-tuning job to update the status for.
        """
        if self.checkpoint.open_ai_fine_tune_job is None:
            raise ValueError("No fine-tuning job to update status for.")

        job = self._client.fine_tuning.jobs.retrieve(
            self.checkpoint.open_ai_fine_tune_job.id
        )

        self.checkpoint.open_ai_fine_tune_job.status = job.status
        self.checkpoint.open_ai_fine_tune_job.open_ai_data = job.model_dump()

        if verbose:
            logger.info(f"📈 Fine-tuning job status: {job.status}")
            logger.info("🔄 Retrieving fine-tuning events.")

        most_recent_events = self._read_until_last_checkpoint()

        # NOTE: hack, we re-write all events for now because of the way
        # that certain events could no longer exist in the list returned
        # from Azure.

        self.checkpoint.open_ai_fine_tune_job_events = []

        # Because this represents the most recent events, starting with
        # the earliest (descending time), we read them in reverse order and
        # continue to build our ascending time list of events
        for event_dict in reversed(most_recent_events):
            event = FineTuningEvent(
                id=event_dict["id"],
                created_at=event_dict["created_at"],
                message=event_dict["message"],
                object=event_dict["object"],
                open_ai_data=event_dict,
            )
            self.checkpoint.open_ai_fine_tune_job_events.append(event)

        return job.status == "succeeded"

    def wait_for_fine_tune_job(
        self, checkpoint_save_path: Optional[str] = None
    ) -> None:
        """
        Wait for an existing fine-tuning job to complete.
        Each time this method is called, the full event list of the fine-tuning job will be retrieved and
        logged. If a `checkpoint_save_path` is provided, the checkpoint will be saved to that location after
        checking the remote status.

        Args:
            checkpoint_save_path: If set, the checkpoint will be saved to this location after each check of the
                fine-tuning job status.
        """
        if self.checkpoint.open_ai_fine_tune_job is None:
            logger.info("🤔 No fine-tuning job to wait for.")
            return

        logger.info("🕒 Waiting for fine-tuning job to complete.")

        # Each call to this method will log every event seen so far
        last_event_idx = 0

        while True:
            is_done = self.update_fine_tune_job_status(verbose=False)
            if checkpoint_save_path is not None:
                self.save_checkpoint(checkpoint_save_path)
            for event in self.checkpoint.open_ai_fine_tune_job_events[last_event_idx:]:
                logger.info(f"📅 {event.created_at_str}: {event.message}")
                last_event_idx += 1
            if is_done:
                logger.info("✅ Fine-tuning job completed.")
                return
            elif self.checkpoint.open_ai_fine_tune_job.status in (
                "failed",
                "cancelled",
            ):
                logger.info("❌ Fine-tuning job did not complete.")
                return

            time.sleep(5)

    def graph_training_metrics(self) -> None:
        """
        Graph the training and validation metrics (train loss, validation loss) for the fine-tuning job.

        This method utilizes the up-to-date fine-tuning job events to graph the training and validation loss. These
        events are stored and updated on the checkpoint via the `update_fine_tune_job_status` method.

        NOTE: This method requires the `plotly` package to be installed. You can install it with `pip install plotly`.

        Raises:
            ImportError: If the `plotly` package is not installed.
            RuntimeError: If there is no fine-tuning job to graph metrics for or if no metrics are found in the events.
        """
        try:
            import plotly.express as px
            import plotly.graph_objects as go
        except ImportError:
            raise ImportError(
                "The `plotly` package is required to graph metrics. Please install it with `pip install plotly`."
            )
        if not self.checkpoint.open_ai_fine_tune_job:
            raise RuntimeError("No fine-tuning job to graph metrics for.")

        if not self.checkpoint.open_ai_fine_tune_job_events:
            raise RuntimeError("No fine-tuning job events to graph metrics for.")

        # First we accumulate our metrics from logs into a DataFrame
        metrics = []
        for event in self.checkpoint.open_ai_fine_tune_job_events:
            if not event.open_ai_data:
                continue

            if event.open_ai_data.get("type") == "metrics":
                metrics.append(event.open_ai_data["data"])

        if not metrics:
            raise RuntimeError("No metrics found in fine-tuning job events.")

        df = pd.DataFrame(metrics)

        # Handle missing values by filling with NaN (if not already)
        df = df.replace(r"^\s*$", pd.NA, regex=True)

        # Convert columns to numeric, errors='coerce' will replace non-convertible values with NaN
        numeric_columns = df.columns.drop("step")
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

        # Plotting Training and Validation Loss
        fig_loss = go.Figure()
        fig_loss.add_trace(
            go.Scatter(
                x=df["step"],
                y=df["train_loss"],
                mode="lines+markers",
                name="Training Loss",
                line=dict(color="blue"),
            )
        )

        if "valid_loss" in df.columns:
            fig_loss.add_trace(
                go.Scatter(
                    x=df["step"],
                    y=df["valid_loss"],
                    mode="lines+markers",
                    name="Validation Loss",
                    line=dict(color="red"),
                )
            )

        # Add Full Validation Loss if available
        if "full_valid_loss" in df.columns and df["full_valid_loss"].notna().any():
            fig_loss.add_trace(
                go.Scatter(
                    x=df["step"],
                    y=df["full_valid_loss"],
                    mode="markers",
                    name="Full Validation Loss",
                    marker=dict(color="green", size=10, symbol="star"),
                )
            )

        fig_loss.update_layout(
            title="Training and Validation Loss over Steps",
            xaxis_title="Step",
            yaxis_title="Loss",
            legend_title="Metrics",
            template="plotly_dark",
        )

        # Plotting Training and Validation Accuracy
        fig_acc = go.Figure()
        fig_acc.add_trace(
            go.Scatter(
                x=df["step"],
                y=df["train_mean_token_accuracy"],
                mode="lines+markers",
                name="Training Accuracy",
                line=dict(color="blue"),
            )
        )
        if "valid_mean_token_accuracy" in df.columns:
            fig_acc.add_trace(
                go.Scatter(
                    x=df["step"],
                    y=df["valid_mean_token_accuracy"],
                    mode="lines+markers",
                    name="Validation Accuracy",
                    line=dict(color="red"),
                )
            )

        # Add Full Validation Accuracy if available
        if (
            "full_valid_mean_token_accuracy" in df.columns
            and df["full_valid_mean_token_accuracy"].notna().any()
        ):
            fig_acc.add_trace(
                go.Scatter(
                    x=df["step"],
                    y=df["full_valid_mean_token_accuracy"],
                    mode="markers",
                    name="Full Validation Accuracy",
                    marker=dict(color="green", size=10, symbol="star"),
                )
            )

        fig_acc.update_layout(
            title="Training and Validation Accuracy over Steps",
            xaxis_title="Step",
            yaxis_title="Mean Token Accuracy",
            legend_title="Metrics",
            template="plotly_dark",
        )

        # Show the plots
        fig_loss.show()
        fig_acc.show()

    def create_chat_completitions(
        self,
        model: str,
        *,
        messages: Union[list[dict], list[list]],
        model_params: Optional[dict] = None,
        parse_json: bool = False,
    ) -> Union[list[str], list[dict]]:
        """
        Helper method for running multiple chat completions. In order to use this method you must have
        completed a fine-tuning job and deployed the fine-tuning job to an endpoint. Deploying the fine-tuned
        model is currently not supported with the Gretel SDK. To do this we reccomend using the Azure Cloud Shell
        to deploy the model: https://learn.microsoft.com/en-us/cli/azure/cognitiveservices/account/deployment?view=azure-cli-latest#az-cognitiveservices-account-deployment-create()

        Args:
            model: The name of the fine-tuned model to use for completions. This name should match the `deployment-name` that was
                used when deploying the model.
            messages: A list of OpenAI chat messages to generate completions for. Each message can be a list of dictionaries or a list. If you used
                the `OpenAIFormatter` to create your fine-tuning data, you can pass the same input data here. If a list of dictionaries is provided,
                each dictionary must contain a 'messages' key which contains an array of messages.
            model_params: The model hyperparamters to use for generating completions as a dict. For example: {'temperature': 0.5}
            parse_json: When True, the response will be parsed as JSON. If the response is not valid JSON, a warning will be logged.
                This method will return a list of dictinaries instead of strings.

        Returns:
            list: A list of completions. Each completion will be a string or a dictionary if `parse_json` is True.

        Raises:
            RuntimeError: If there is no fine-tuned model to generate completions with.
            ValueError: If a list of dictionaries is provided and each dictionary does not contain a 'messages' key.
        """

        if model_params is None:
            model_params = {}

        if self.checkpoint.open_ai_fine_tuned_model_id is None:
            raise RuntimeError(
                "There is no fine-tuned model to generate completions with."
            )

        if not messages:
            return []

        if isinstance(messages[0], dict):
            if "messages" not in messages[0]:
                raise ValueError(
                    "If a list of dictionaries is provided, each dictionary must contain a 'messages' key."
                )
            messages = [msg["messages"] for msg in messages]  # type: ignore

        outputs = []

        for message in tqdm.tqdm(
            messages, total=len(messages), desc="Generating chat completions"
        ):
            response = self._client.chat.completions.create(
                messages=message,
                model=model,
                **model_params,
            )
            response_str = response.choices[0].message.content
            if parse_json:
                parsed_json_str = _clean_json_string(response_str)
                try:
                    outputs.append(json.loads(parsed_json_str))
                except json.JSONDecodeError:
                    logger.warning(
                        f"Failed to parse JSON from response: {parsed_json_str}"
                    )
                    continue
            else:
                outputs.append(response_str)

        return outputs
