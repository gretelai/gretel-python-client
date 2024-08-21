import datetime

from unittest import mock
from unittest.mock import call, MagicMock

import pytest

from dateutil.tz import tzutc

from gretel_client.rest_v1.models import (
    GetLogResponse,
    LogEnvelope,
    SearchWorkflowTasksResponse,
    WorkflowRun,
    WorkflowTask,
)
from gretel_client.workflows.logs import (
    LogWorker,
    TaskManager,
    WaitTimeExceeded,
    WORKFLOW_TASK_SEARCH_KEY,
)
from gretel_client.workflows.runner_mode import RunnerMode
from gretel_client.workflows.status import Status


@pytest.fixture
def workflows_api() -> MagicMock:
    return MagicMock()


@pytest.fixture
def logs_api() -> MagicMock:
    return MagicMock()


def transition(task: WorkflowTask, status: str) -> WorkflowTask:
    """Creates a copy of the workflow task with the new status applied"""
    updated_task = task.copy(deep=True)
    updated_task.status = status
    return updated_task


@mock.patch("gretel_client.workflows.logs.LogWorker.for_workflow_task")
def test_task_manager(
    for_workflow_task: MagicMock,
    workflows_api: MagicMock,
    logs_api: MagicMock,
):
    log_printer = MagicMock()
    wt_1 = WorkflowTask(
        workflow_run_id="wr_1",
        id="wt_1",
        project_id="proj_1",
        log_location="",
        action_name="helloworld_producer",
        action_type="helloworld_producer",
        status=Status.RUN_STATUS_CREATED.value,
        error_msg="",
        created_by="user_1",
        created_at=datetime.datetime.now(),
    )
    workflows_api.search_workflow_tasks.return_value = SearchWorkflowTasksResponse(
        tasks=[wt_1], total=1
    )

    workflows_api.get_workflow_run.side_effect = [
        WorkflowRun(
            workflow_id="w_1",
            id="wr_1",
            project_id="proj_1",
            runner_mode=RunnerMode.RUNNER_MODE_CLOUD.value,
            created_by="user_1",
            created_at=datetime.datetime.now(),
            status=Status.RUN_STATUS_CREATED.value,
        ),
        WorkflowRun(
            workflow_id="w_1",
            id="wr_1",
            project_id="proj_1",
            runner_mode=RunnerMode.RUNNER_MODE_CLOUD.value,
            created_by="user_1",
            created_at=datetime.datetime.now(),
            status=Status.RUN_STATUS_ACTIVE.value,
        ),
        WorkflowRun(
            workflow_id="w_1",
            id="wr_1",
            project_id="proj_1",
            runner_mode=RunnerMode.RUNNER_MODE_CLOUD.value,
            created_by="user_1",
            created_at=datetime.datetime.now(),
            status=Status.RUN_STATUS_COMPLETED.value,
        ),
    ]

    task_manager = TaskManager.for_workflow_run(
        "wr_1", workflows_api, logs_api, log_printer
    )
    task_manager._poll_frequency_seconds = 0

    task_worker = MagicMock()
    task_worker.running.return_value = False
    for_workflow_task.return_value = task_worker

    task_manager.start()

    workflows_api.get_workflow_run.assert_has_calls(
        [
            call(workflow_run_id="wr_1"),
            call(workflow_run_id="wr_1"),
            call(workflow_run_id="wr_1"),
        ]
    )
    for_workflow_task.assert_called_once_with(
        wt_1, workflows_api, logs_api, log_printer
    )
    task_worker.start.assert_called_once()


@mock.patch("gretel_client.workflows.logs.LogWorker.for_workflow_task")
def test_task_manager_wait(
    for_workflow_task: MagicMock,
    workflows_api: MagicMock,
    logs_api: MagicMock,
):
    task_manager = TaskManager.for_workflow_run(
        "wr_1", workflows_api, logs_api, MagicMock()
    )
    task_manager._poll_frequency_seconds = 1

    with pytest.raises(WaitTimeExceeded):
        task_manager.start(wait=5)


def test_log_worker(workflows_api: MagicMock, logs_api: MagicMock):
    logger = MagicMock()
    wt_1 = WorkflowTask(
        workflow_run_id="wr_1",
        id="wt_1",
        project_id="proj_1",
        log_location="",
        action_name="helloworld_producer",
        action_type="helloworld_producer",
        status=Status.RUN_STATUS_CREATED.value,
        error_msg="",
        created_by="user_1",
        created_at=datetime.datetime.now(),
    )

    logs_api.get_logs.side_effect = [
        GetLogResponse(
            lines=[
                LogEnvelope(
                    msg="log_1",
                    ts=datetime.datetime(
                        2023, 5, 20, 0, 16, 28, 636000, tzinfo=tzutc()
                    ),
                )
            ],
            next_page_token="page_2",
        ),
        GetLogResponse(
            lines=[
                LogEnvelope(
                    msg="log_2",
                    ts=datetime.datetime(
                        2023, 5, 20, 0, 16, 28, 646000, tzinfo=tzutc()
                    ),
                )
            ],
            next_page_token="page_3",
        ),
        GetLogResponse(lines=[], next_page_token="page_0"),
        GetLogResponse(lines=[], next_page_token="page_0"),
        GetLogResponse(lines=[], next_page_token="page_0"),
        GetLogResponse(lines=[], next_page_token="page_0"),
        GetLogResponse(lines=[], next_page_token="page_0"),
        GetLogResponse(lines=[], next_page_token="page_0"),
    ]

    workflows_api.get_workflow_task.side_effect = [
        wt_1,
        transition(wt_1, Status.RUN_STATUS_PENDING.value),
        transition(wt_1, Status.RUN_STATUS_PENDING.value),
        transition(wt_1, Status.RUN_STATUS_ACTIVE.value),
        transition(wt_1, Status.RUN_STATUS_ACTIVE.value),
        transition(wt_1, Status.RUN_STATUS_COMPLETED.value),
        transition(wt_1, Status.RUN_STATUS_COMPLETED.value),
    ]

    worker = LogWorker.for_workflow_task(wt_1, workflows_api, logs_api, logger)
    worker._poll_frequency_seconds = 0

    worker.start()
    worker.wait(timeout=30)

    assert logs_api.get_logs.call_count == 8

    logs_api.get_logs.assert_has_calls(
        [
            call(query=f"{WORKFLOW_TASK_SEARCH_KEY}:wt_1", limit=1_000),
            call(
                query=f"{WORKFLOW_TASK_SEARCH_KEY}:wt_1",
                limit=1_000,
                page_token="page_2",
            ),
            call(
                query=f"{WORKFLOW_TASK_SEARCH_KEY}:wt_1",
                limit=1_000,
                page_token="page_3",
            ),
            call(query=f"{WORKFLOW_TASK_SEARCH_KEY}:wt_1", limit=1_000),
            call(query=f"{WORKFLOW_TASK_SEARCH_KEY}:wt_1", limit=1_000),
            call(query=f"{WORKFLOW_TASK_SEARCH_KEY}:wt_1", limit=1_000),
            call(query=f"{WORKFLOW_TASK_SEARCH_KEY}:wt_1", limit=1_000),
            call(query=f"{WORKFLOW_TASK_SEARCH_KEY}:wt_1", limit=1_000),
        ]
    )

    assert logger.log.call_count == 2
    assert not worker.running()
