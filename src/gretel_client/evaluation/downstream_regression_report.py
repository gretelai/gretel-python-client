from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Union

from gretel_client.config import ClientConfig, RunnerMode
from gretel_client.evaluation.reports import (
    BaseReport,
    DEFAULT_RECORD_COUNT,
    ReportDictType,
)
from gretel_client.projects.common import DataSourceTypes, RefDataTypes
from gretel_client.projects.projects import Project


class DownstreamRegressionReport(BaseReport):
    """Represents a Quality Report. This class can be used to create a report.

    Args:
        project: Optional project associated with the report. If no project is passed, a temp project (:obj:`gretel_client.projects.projects.tmp_project`) will be used.
        name: Optional name of the model. If no name is provided, a default name will be used.
        data_source: Data source used for the report.
        ref_data: Reference data used for the report.
        test_data: Optional data set used as a test set for training models used in report.
        output_dir: Optional directory path to write the report to. If the directory does not exist, the path will be created for you.
        runner_mode: Determines where to run the model. See :obj:`gretel_client.config.RunnerMode` for a list of valid modes. Manual mode is not explicitly supported.
        target: The field which the downstream regression models are trained to predict. Must be present in both data_source and ref_data.
        holdout: The ratio of data to hold out from ref_data (i.e., your real data) as a test set. Must be between 0.0 and 1.0.
        models: The list of regression models to train. If absent or an empty list, use all supported models.
        metric: The metric used to sort regression results. "R2" by default.
        record_count: Number of rows to use from the data sets, 5000 by default. A value of 0 means "use as many rows/columns
            as possible." We still attempt to maintain parity between the data sets for "fair" comparisons,
            i.e. we will use min(len(train), len(synth)), e.g.
        session: The client session to use, or ``None`` to use the session associated with the project
            (if any), or the default session otherwise.
    """

    _model_dict: dict = {
        "schema_version": "1.0",
        "name": "evaluate-downstream-regression-model",
        "models": [
            {
                "evaluate": {
                    "task": {"type": "regression"},
                    "data_source": "__tmp__",
                    "params": {
                        "target": None,
                        "holdout": 0.2,
                        "models": [],
                        "metric": "r2",
                    },
                }
            }
        ],
    }

    @property
    def model_config(self) -> dict:
        return self._model_dict

    @property
    def base_artifact_name(self) -> str:
        return "regression_report"

    # TODO alternative with config file??
    def __init__(
        self,
        *,
        target: str,
        holdout: float = 0.2,
        models: List[str] = [],
        metric: str = "r2",
        project: Optional[Project] = None,
        name: Optional[str] = None,
        data_source: DataSourceTypes,
        ref_data: RefDataTypes,
        test_data: Optional[RefDataTypes] = None,
        output_dir: Optional[Union[str, Path]] = None,
        runner_mode: Optional[RunnerMode] = None,
        record_count: Optional[int] = DEFAULT_RECORD_COUNT,
        session: Optional[ClientConfig] = None,
    ):
        project, session = BaseReport.resolve_session(project, session)
        runner_mode = runner_mode or session.default_runner

        if runner_mode == RunnerMode.MANUAL:
            raise ValueError(
                "Cannot use manual mode. Please use CLOUD, LOCAL, or HYBRID."
            )

        if not target:
            raise ValueError("A nonempty target is required.")

        if holdout <= 0 or holdout >= 1.0:
            raise ValueError("Holdout must be between 0.0 and 1.0.")

        # Update the model name if one was provided
        if name is not None:
            self._model_dict["name"] = name

        # Update the report params in our config
        params = self._model_dict["models"][0]["evaluate"]["params"]
        params["target"] = target
        params["holdout"] = holdout
        params["models"] = models
        params["metric"] = metric

        # Update row count
        params["sqs_report_rows"] = record_count

        super().__init__(
            project=project,
            data_source=data_source,
            ref_data=ref_data,
            test_data=test_data,
            output_dir=output_dir,
            runner_mode=runner_mode,
            session=session,
        )

    def peek(self) -> Optional[ReportDictType]:
        super()._check_model_run()
        # Will return dict {"field": "average_metric_difference", "value": \d\d}
        if self._report_dict is not None:
            _summary = self._report_dict.get("summary")
            if _summary:
                return _summary[0]
