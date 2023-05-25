from gretel_client.rest_v1.model.workflow_run import WorkflowRun

statuses = WorkflowRun.allowed_values[("status",)]

TERMINAL_STATES = {
    statuses["ERROR"],
    statuses["LOST"],
    statuses["COMPLETED"],
    statuses["CANCELLED"],
}
