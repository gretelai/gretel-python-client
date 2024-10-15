"""
Interfaces for using Gretel with Google BigQuery. This module
assumes that the `bigframes` package is already installed as
a transitive dependency.
"""

from __future__ import annotations

from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, TYPE_CHECKING, Union

import pandas as pd

from gretel_client.analysis_utils import display_dataframe_in_notebook
from gretel_client.gretel.job_results import (
    GenerateJobResults,
    TrainJobResults,
    TransformResults,
)
from gretel_client.inference_api.base import InferenceAPIModelType

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
    with bpd.option_context("compute.extra_query_labels", labels):
        yield


class BigQueryTransformResults(TransformResults):
    """
    Should not be used directly.

    Stores metadata and a transformed BigFrames DataFrame
    that was created from a Gretel Transforms job.
    """

    transformed_df: Optional[bpd.DataFrame] = None
    """A BigQuery DataFrame of the transformed table. This will
    not be populated until the trasnforms job succeeds."""

    def refresh(self) -> None:
        """Refresh the transform job result attributes."""
        super().refresh()
        if self.transformed_df is not None and isinstance(
            self.transformed_df, pd.DataFrame
        ):
            self.transformed_df = bpd.read_pandas(self.transformed_df)  # type: ignore


class ModelTrainResult(TrainJobResults):
    """
    Should not be used directly.

    An instance of this class is returned when creating
    a new synthetic model or retrieving an existing one.
    """

    def fetch_report_synthetic_data(self) -> bpd.DataFrame:  # type: ignore
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
                self.synthetic_data = bpd.read_pandas(self.synthetic_data)  # type: ignore


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

    def submit_transforms(self, *args, **kwargs) -> BigQueryTransformResults:
        return self.submit_transform(*args, **kwargs)

    def submit_transform(
        self,
        config: Union[str, Path, dict],
        dataframe: bpd.DataFrame,
        *,
        wait: bool = False,
        **kwargs,
    ) -> BigQueryTransformResults:
        """
        Run a Gretel Transform job against the provided
        dataframe. A Transform model will be created and
        then immediately used to apply row, column, or cell
        level transforms against a dataframe.
        """
        with _bq_job_label(JobLabel.RUN_TRANSFORMS):
            local_df = dataframe.to_pandas()
            results = self._gretel.submit_transform(
                config, data_source=local_df, wait=wait, **kwargs
            )
            bq_results = BigQueryTransformResults(
                project=results.project,
                model=results.model,
                transform_logs=results.transform_logs,
                transformed_df=results.transformed_df,
                transformed_data_link=results.transformed_data_link,
            )
            bq_results.refresh()
            return bq_results

    def fetch_transforms_results(self, model_id: str) -> BigQueryTransformResults:
        return self.fetch_transform_results(model_id)

    def fetch_transform_results(self, model_id: str) -> BigQueryTransformResults:
        """
        Given a Transforms model ID, return a TransformsResult in order
        to retrieve transformed data and check job status.
        """
        with _bq_job_label(JobLabel.LOAD_TRANSFORMED):
            local_results = self._gretel.fetch_transform_results(model_id)
            bq_results = BigQueryTransformResults(
                project=local_results.project,
                model=local_results.model,
                transform_logs=local_results.transform_logs,
                transformed_df=local_results.transformed_df,
                transformed_data_link=local_results.transformed_data_link,
            )
            bq_results.refresh()
            return bq_results

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
        self._navigator_map[name] = self._gretel.factories.initialize_navigator_api(  # type: ignore
            model_type=InferenceAPIModelType.TABULAR, **kwargs
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
            return bpd.read_pandas(local_df)  # type: ignore

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
        local_seed_data = None
        with _bq_job_label(JobLabel.NAVIGATOR_EDIT):
            if isinstance(seed_data, bpd.DataFrame):
                local_seed_data = seed_data.to_pandas()
            else:
                local_seed_data = seed_data
            nav_instance = self._check_navigator_instance(name)
            local_df = nav_instance.edit(
                *args, stream=False, seed_data=local_seed_data, **kwargs
            )
            return bpd.read_pandas(local_df)  # type: ignore

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
