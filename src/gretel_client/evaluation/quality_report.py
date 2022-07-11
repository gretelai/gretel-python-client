from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

from gretel_client.config import RunnerMode
from gretel_client.evaluation.reports import BaseReport, ReportDictType
from gretel_client.projects.common import DataSourceTypes, RefDataTypes
from gretel_client.projects.projects import Project


class QualityReport(BaseReport):
    """Represents a Quality Report. This class can be used to create a report.

    Args:
        project: Optional project associated with the report. If no project is passed, a temp project (:obj:`gretel_client.projects.projects.tmp_project`) will be used.
        data_source: Data source used for the report.
        ref_data: Reference data used for the report.
        output_dir: Optional directory path to write the report to. If the directory does not exist, the path will be created for you.
        runner_mode: Determines where to run the model. See :obj:`gretel_client.config.RunnerMode` for a list of valid modes. Manual mode is not explicitly supported.
    """

    model_config: str = "evaluate/default"

    def __init__(
        self,
        *,
        project: Optional[Project] = None,
        data_source: DataSourceTypes,
        ref_data: RefDataTypes,
        output_dir: Optional[Union[str, Path]] = None,
        runner_mode: Optional[RunnerMode] = RunnerMode.CLOUD,
    ):
        if not isinstance(runner_mode, RunnerMode):
            raise ValueError("Invalid runner_mode type, must be RunnerMode enum.")

        if runner_mode == RunnerMode.MANUAL:
            raise ValueError("Cannot use manual mode. Please use CLOUD or LOCAL.")

        super().__init__(project, data_source, ref_data, output_dir, runner_mode)

    def peek(self) -> ReportDictType:
        super()._check_model_run()
        return self._report_dict["synthetic_data_quality_score"]
