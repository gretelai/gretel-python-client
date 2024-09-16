"""
Interfaces for using Gretel with Google Big Query. This module
assumes that the `bigframes` package is already installed as
a transitive dependency.
"""

from __future__ import annotations

from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, TYPE_CHECKING, Union

import pandas as pd
import yaml

from gretel_client.analysis_utils import display_dataframe_in_notebook
from gretel_client.gretel.artifact_fetching import fetch_model_logs
from gretel_client.gretel.job_results import (
    GenerateJobResults,
    GretelJobResults,
    TrainJobResults,
)
from gretel_client.helpers import poll
from gretel_client.projects.jobs import Status

if TYPE_CHECKING:
    from gretel_client import Gretel
    from gretel_client.inference_api.tabular import TabularInferenceAPI


try:
    import bigframes.pandas as bpd
except ImportError:
    raise RuntimeError(
        "Please install `bigframes` in order to use the BigQuery module."
    )


class JobLabel(Enum):
    RUN_TRANSFORMS = "run-transforms"
    LOAD_TRANSFORMED = "load-transformed-data"
    TRAIN_MODEL = "train-model"
    LOAD_REPORT_DATA = "load-report-data"
    GENERATE_DATA = "generate-data"
    LOAD_GENERATED_DATA = "load-generated-data"
    NAVIGATOR_GENERATE = "navigator-generate"
    NAVIGATOR_EDIT = "navigator-edit"


@contextmanager
def _bq_job_label(action: JobLabel) -> Generator[Any, Any, Any]:
    labels = {"application-name": "gretel-sdk"}
    labels["action"] = action.value
    try:
        with bpd.option_context("compute.extra_query_labels", labels):
            yield
    except Exception:
        yield


class TransformsResult(GretelJobResults):
    """
    Should not be used directly.

    Stores metadata and a transformed BigFrames DataFrame
    that was created from a Gretel Transforms job.
    """

    transform_logs: Optional[List[str]] = None
    """Logs created during Transform job."""

    transformed_df: Optional[bpd.DataFrame] = None
    """A BigQuery DataFrame of the transformed table. This will
    not be populated until the trasnforms job succeeds."""

    transformed_data_link: Optional[str] = None
    """URI to the transformed data (as a flat file). This will 
    not be populated until the transforms job succeeds."""

    @property
    def model_url(self) -> str:
        """
        The Gretel Console URL for the Transform model.
        """
        return f"{self.project_url}/models/{self.model_id}/data"

    @property
    def model_config(self) -> str:
        """
        The Transforms config that was used.
        """
        return yaml.safe_dump(self.model.model_config)

    @property
    def job_status(self) -> Status:
        """The current status of the transform job."""
        self.model.refresh()
        return self.model.status

    def refresh(self) -> None:
        """Refresh the transform job result attributes."""
        if self.job_status == Status.COMPLETED:
            if self.transformed_data_link is None:
                self.transformed_data_link = self.model.get_artifact_link(
                    "data_preview"
                )
            if self.transformed_df is None:
                with _bq_job_label(JobLabel.LOAD_TRANSFORMED):
                    with self.model.get_artifact_handle("data_preview") as fin:
                        dataframe = pd.read_csv(fin)
                        self.transformed_df = bpd.read_pandas(dataframe)

        # We can fetch model logs no matter what
        self.transform_logs = fetch_model_logs(self.model)

    def wait_for_completion(self) -> None:
        """Wait for transforms job to finish running."""
        if self.job_status != Status.COMPLETED:
            poll(self.model, verbose=False)
            self.refresh()


class ModelTrainResult(TrainJobResults):
    """
    Should not be used directly.

    An instance of this class is returned when creating
    a new synthetic model or retrieving an existing one.
    """

    def fetch_report_synthetic_data(self) -> bpd.DataFrame:
        """
        Fetch the synthetic BigQuery DataFrame that was created
        as part of the model training process. This DataFrame
        is what is used to create the model report.
        """
        with _bq_job_label(JobLabel.LOAD_REPORT_DATA):
            local_df = super().fetch_report_synthetic_data()
            return bpd.read_pandas(local_df)


class ModelGenerationResult(GenerateJobResults):
    """
    Should not be used directly.

    An instance of this class is returned when generating
    more data from an existing model or retrieving generated
    data from an existing model.
    """

    synthetic_data: Optional[bpd.DataFrame] = None

    def refresh(self) -> None:
        super().refresh()
        if self.synthetic_data is not None:
            with _bq_job_label(JobLabel.LOAD_GENERATED_DATA):
                self.synthetic_data = bpd.read_pandas(self.synthetic_data)


class BigFrames:
    """
    This interface enables using Gretel Transforms, Gretel Synthetics,
    and Gretel Navigator with Google BigFrames.

    Args:
        gretel: An instance of the Gretel interface. This instance
            should be imported from `from gretel_client import Gretel`.
    """

    _gretel: Gretel
    _navigator_map: Dict[str, TabularInferenceAPI]

    def __init__(self, gretel: Gretel):
        self._gretel = gretel
        self._navigator_map = {}

    def submit_transforms(
        self, config: str, dataframe: bpd.DataFrame
    ) -> TransformsResult:
        """
        Run a Gretel Transforms job against the provided
        dataframe. A Transforms model will be created and
        then immediately used to apply row, column, or cell
        level transforms against a dataframe.
        """
        config_yaml = yaml.safe_load(config)
        with _bq_job_label(JobLabel.RUN_TRANSFORMS):
            local_df = dataframe.to_pandas()
            gretel_project = self._gretel.get_project()
            model = gretel_project.create_model_obj(
                model_config=config_yaml, data_source=local_df
            )
            model.submit()
            return TransformsResult(project=gretel_project, model=model)

    def fetch_transforms_results(self, model_id: str) -> TransformsResult:
        """
        Given a Transforms model ID, return a TransformsResult in order
        to retrieve transformed data and check job status.
        """
        self._gretel._assert_project_is_set()
        model = self._gretel.fetch_model(model_id)
        results = TransformsResult(project=self._gretel.get_project(), model=model)
        results.refresh()
        return results

    def submit_train(
        self,
        base_config: Union[str, Path, dict],
        *,
        dataframe: bpd.DataFrame,
        wait: bool = False,
        **kwargs,
    ) -> ModelTrainResult:
        """
        Fine-tune a Gretel model on an existing BigFrames DataFrame

        Args:
            base_config: Base Gretel config name, yaml file path, yaml string, or dict.
            dataframe: The BigFrames DataFrame to use as the training data.
            wait: If True, wait for the job to complete before returning.

        NOTE: The remaining kwargs are the same ones that are supported by
            `Gretel.submit_train()`
        """
        with _bq_job_label(JobLabel.TRAIN_MODEL):
            results = self._gretel.submit_train(
                base_config=base_config,
                data_source=dataframe.to_pandas(),
                wait=wait,
                **kwargs,
            )

        return ModelTrainResult(
            project=results.project,
            model=results.model,
            report=results.report,
            model_logs=results.model_logs,
            model_config=results.model_config,
        )

    def fetch_train_job_results(self, model_id: str) -> ModelTrainResult:
        """
        Given a Gretel Model ID, return a ModelTrainResult instance. This
        allows for checking model training status, retrieving model quality
        report(s) and retrieving generated data.
        """
        self._gretel._assert_project_is_set()
        model = self._gretel.fetch_model(model_id)
        results = ModelTrainResult(project=self._gretel.get_project(), model=model)
        results.refresh()
        return results

    def submit_generate(
        self,
        model_id: str,
        *,
        seed_data: Optional[bpd.DataFrame] = None,
        wait: bool = False,
        **kwargs,
    ) -> ModelGenerationResult:
        """
        Given a fine-tuned model ID, request the generation of more data.

        If the model supports conditional generation, a partial DataFrame
        may be provided as input to inference. This method supports
        the same additional kwargs as `Gretel.submit_generate()`.
        """
        with _bq_job_label(JobLabel.GENERATE_DATA):
            local_seed_data = None
            if seed_data is not None:
                local_seed_data = seed_data.to_pandas()

            results = self._gretel.submit_generate(
                model_id,
                seed_data=local_seed_data,
                wait=wait,
                **kwargs,
            )

        return ModelGenerationResult(
            project=results.project,
            model=results.model,
            record_handler=results.record_handler,
            synthetic_data=results.synthetic_data,
            synthetic_data_link=results.synthetic_data_link,
        )

    def fetch_generate_job_results(
        self, model_id: str, record_id: str
    ) -> ModelGenerationResult:
        """
        Given the Model ID and Job ID (record ID), return
        a `ModelGenerationResult` instance which allows for
        checking the generation job status and retrieving
        the generated data.
        """
        self._gretel._assert_project_is_set()
        model = self._gretel.fetch_model(model_id)
        results = ModelGenerationResult(
            model=model,
            project=self._gretel.get_project(),
            record_handler=model.get_record_handler(record_id),
        )
        results.refresh()
        return results

    def init_navigator(self, name: str, **kwargs) -> None:
        """
        Create an instance of Gretel's Navigator API and store
        it on this instance. Only Navigator's Tabular mode is supported.

        Args:
            name: The name of the Navigator instance you want to use. When
                using this Navigator instance, you will refernce this
                name.

        The additional **kwargs are identical to what is supported
        in `Gretel.factories.initialize_navigator_api()`.

        """
        self._navigator_map[name] = self._gretel.factories.initialize_navigator_api(
            model_type="tabular", **kwargs
        )

    def _check_navigator_instance(self, name: str) -> TabularInferenceAPI:
        nav_instance = self._navigator_map.get(name)
        if nav_instance is None:
            raise ValueError(
                f"The provided `name` is invalid. Valid options are: {list(self._navigator_map.keys())}. "
                "If the options are empty, please create a Navigator instance with the `init_navigator()` method."
            )
        return nav_instance

    def navigator_generate(self, name: str, *args, **kwargs) -> bpd.DataFrame:
        """
        Generate a BigQuery Table using Gretel Navigator.

        Args:
            name: The name of a registered Navigator instance. This should have been
            created using the `init_navigator()` method.

        The other *args and **kwargs are what is supported by `TabularInferenceAPI.generate()`.
        Streaming responses are not supported at this time.
        """
        with _bq_job_label(JobLabel.NAVIGATOR_GENERATE):
            nav_instance = self._check_navigator_instance(name)
            local_df = nav_instance.generate(*args, stream=False, **kwargs)
            return bpd.read_pandas(local_df)

    def navigator_edit(
        self,
        name: str,
        *args,
        seed_data: Union[bpd.DataFrame, List[dict[str, Any]]],
        **kwargs,
    ) -> bpd.DataFrame:
        """
        Edit a BigQuery Table using Gretel Navigator.

        Args:
            name: The name of a registered Navigator instance. This should have been
            created using the `init_navigator()` method.

        The other *args and **kwargs are what is supported by `TabularInferenceAPI.edit()`.
        Streaming responses are not supported at this time.
        """
        with _bq_job_label(JobLabel.NAVIGATOR_EDIT):
            if isinstance(seed_data, bpd.DataFrame):
                seed_data = seed_data.to_pandas()
            nav_instance = self._check_navigator_instance(name)
            local_df = nav_instance.edit(
                *args, stream=False, seed_data=seed_data, **kwargs
            )
            return bpd.read_pandas(local_df)

    def display_dataframe_in_notebook(
        self, dataframe: bpd.DataFrame, settings: Optional[dict] = None
    ) -> None:
        """
        Display a BigFrames DataFrame in a Notebook.

        Args:
            dataframe: A BigFrames DataFrame
            settings: Any valid settings that are accepted by the
                method `pandas.DataFrame.style.set_properties`
        """
        display_dataframe_in_notebook(dataframe.to_pandas(), settings)
