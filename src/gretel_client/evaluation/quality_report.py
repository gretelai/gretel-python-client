from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Union

from gretel_client.config import ClientConfig, RunnerMode
from gretel_client.evaluation.reports import (
    BaseReport,
    DEFAULT_CORRELATION_COLUMNS,
    DEFAULT_RECORD_COUNT,
    DEFAULT_SQS_REPORT_COLUMNS,
    ReportDictType,
)
from gretel_client.projects.common import DataSourceTypes, RefDataTypes
from gretel_client.projects.projects import Project


class QualityReport(BaseReport):
    """Represents a Quality Report. This class can be used to create a report.

    Args:
        project: Optional project associated with the report. If no project is passed, a temp project (:obj:`gretel_client.projects.projects.tmp_project`) will be used.
        name: Optional name of the model. If no name is provided, a default name will be used.
        data_source: Data source used for the report.
        ref_data: Reference data used for the report.
        output_dir: Optional directory path to write the report to. If the directory does not exist, the path will be created for you.
        runner_mode: Determines where to run the model. See :obj:`gretel_client.config.RunnerMode` for a list of valid modes. Manual mode is not explicitly supported.
        record_count: Number of rows to use from the data sets, 5000 by default. A value of 0 means "use as many rows/columns
            as possible." We still attempt to maintain parity between the data sets for "fair" comparisons,
            i.e. we will use min(len(train), len(synth)), e.g.
        correlation_column_count: Similar to record_count, but for number of columns used for correlation calculations.
        column_count: Similar to record_count, but for number of columns used for all other calculations.
        mandatory_columns: Use in conjuction with correlation_column_count and column_count. The columns listed will be included
            in the sample of columns. Any additional requested columns will be selected randomly.
        session: The client session to use, or ``None`` to use the session associated with the project
            (if any), or the default session otherwise.
        test_data: Optional reference data used for the Privacy Metrics of the report.
        run_pii_replay: Determines if PII Replay should be run for the report. If True, the PII replay section will be included in the report.
        pii_entities: List of PII entities to include in the PII Replay section. If None, default entities will be used. This is used only if run_pii_replay is True.
    """

    _model_dict: dict = {
        "schema_version": "1.0",
        "name": "evaluate-quality-model",
        "models": [
            {
                "evaluate": {
                    "data_source": "__tmp__",
                    "params": {},
                },
            }
        ],
    }

    @property
    def model_config(self) -> dict:
        return self._model_dict

    @property
    def base_artifact_name(self) -> str:
        return "report"

    def __init__(
        self,
        *,
        project: Optional[Project] = None,
        name: Optional[str] = None,
        data_source: DataSourceTypes,
        ref_data: RefDataTypes,
        output_dir: Optional[Union[str, Path]] = None,
        runner_mode: Optional[RunnerMode] = None,
        record_count: Optional[int] = DEFAULT_RECORD_COUNT,
        correlation_column_count: Optional[int] = DEFAULT_CORRELATION_COLUMNS,
        column_count: Optional[int] = DEFAULT_SQS_REPORT_COLUMNS,
        mandatory_columns: Optional[List[str]] = [],
        session: Optional[ClientConfig] = None,
        test_data: Optional[RefDataTypes] = None,
        run_pii_replay: bool = False,
        pii_entities: Optional[List[str]] = None,
    ):
        project, session = BaseReport.resolve_session(project, session)
        runner_mode = runner_mode or session.default_runner

        if runner_mode == RunnerMode.MANUAL:
            raise ValueError(
                "Cannot use manual mode. Please use CLOUD, LOCAL, or HYBRID."
            )

        # Update the model name if one was provided
        if name is not None:
            self._model_dict["name"] = name

        # Update row and column counts
        params = self._model_dict["models"][0]["evaluate"]["params"]
        params["sqs_report_rows"] = record_count
        params["correlation_columns"] = correlation_column_count
        params["sqs_report_columns"] = column_count
        params["mandatory_columns"] = mandatory_columns

        if run_pii_replay:
            self._model_dict["models"][0]["evaluate"]["pii_replay"] = {"skip": False}
            if pii_entities:
                self._model_dict["models"][0]["evaluate"]["pii_replay"][
                    "entities"
                ] = pii_entities

        super().__init__(
            project,
            data_source,
            ref_data,
            output_dir,
            runner_mode,
            session=session,
            test_data=test_data,
        )

    def peek(self) -> Optional[ReportDictType]:
        super()._check_model_run()
        if self._report_dict:
            return self._report_dict.get("synthetic_data_quality_score")
