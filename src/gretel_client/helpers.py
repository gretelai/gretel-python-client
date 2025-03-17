import json
import os
import sys

from pathlib import Path
from typing import Dict, Optional, Tuple, Union

from gretel_client.cli.utils.parser_utils import ref_data_factory
from gretel_client.config import ClientConfig, get_logger, get_session_config
from gretel_client.models.config import (
    get_model_type_config,
    get_status_description,
    GPU,
)
from gretel_client.projects.common import WAIT_UNTIL_DONE
from gretel_client.projects.jobs import Job, WaitTimeExceeded
from gretel_client.projects.models import Model
from gretel_client.projects.records import RecordHandler

logger = get_logger(__name__)


def _stderr_print(msg: str) -> None:
    print(msg, file=sys.stderr)


def _progress_bar(
    current: int, total: int, prefix: str = "", suffix: str = "", size: int = 50
):
    x = int(size * current / total)
    print(
        f'\r{prefix}[{"█"*x}{"."*(size-x)}] {current}/{total}{suffix}',
        end="",
        flush=True,
    )


def _is_training(log: dict) -> bool:
    return log.get("stage") == "train" and "epoch" in log.get("ctx", dict())


def _is_generating(log: dict) -> bool:
    return log.get("stage") == "run" and "current_valid_count" in log.get("ctx", dict())


def _verbose_poll(job: Job, wait: int):
    """Polls a ``Model`` or ``RecordHandler``.

    Args:
        job: The job to poll.
        wait: The time to wait for the job to complete.
    """
    _stderr_print("INFO: Starting poller")
    descriptions = get_description_set(job)
    if not descriptions:
        raise ValueError("Cannot fetch Job polling descriptions")
    print(json.dumps(job.print_obj, indent=4))
    try:
        for update in job.poll_logs_status(wait=wait):
            if update.transitioned:
                _stderr_print(
                    f"INFO: Status is {update.status}. {get_status_description(descriptions, update.status, job.runner_mode)}"
                )
            for log in update.logs:
                msg = f"{log['ts']}  {log['msg']}"
                if log["ctx"]:
                    msg += f"\n{json.dumps(log['ctx'], indent=4)}"
                _stderr_print(msg)
            if update.error:
                _stderr_print(f"ERROR: \t{update.error}")

    except WaitTimeExceeded:
        if wait == 0:
            _stderr_print(
                "INFO: Parameter wait=0 was specified, not waiting for the job completion."
            )
        else:
            _stderr_print(
                f"WARN: Job hasn't completed after waiting for {wait} seconds. Exiting the script, but the job will remain running until it reaches the end state."
            )


def _quiet_poll(
    job: Job,
    wait: int,
    num_epochs: Optional[int] = None,
    num_records: Optional[int] = None,
):
    """Polls a ``Model`` or ``RecordHandler`` .

    Args:
        job: The job to poll.
        wait: The time to wait for the job to complete.
        num_epochs: Number of training epochs.
        num_records: Number of text outputs to generate.
    """
    pr_bar = False
    try:
        for update in job.poll_logs_status(wait=wait):
            for log in update.logs:
                if _is_training(log) and num_epochs:
                    _progress_bar(
                        log["ctx"]["epoch"], num_epochs, "Training: ", " epochs."
                    )
                    pr_bar = True
                elif _is_generating(log) and num_records:
                    _progress_bar(
                        log["ctx"]["current_valid_count"],
                        num_records,
                        "Generating: ",
                        " records.",
                    )
                    pr_bar = True
                elif "Using updated model configuration" in log["msg"]:
                    continue
                else:
                    if pr_bar:
                        print()
                        pr_bar = False
                    print(
                        log["msg"],
                        (
                            ", ".join(f"{k} {v}" for k, v in log["ctx"].items())
                            if log["ctx"]
                            else ""
                        ),
                    )
                    if log["stage"] == "pre_run" and not num_records:
                        if "num_records" in log["ctx"]:
                            num_records = log["ctx"]["num_records"]
                    if log["stage"] == "pre" and not num_epochs:
                        if "num_epochs" in log["ctx"]:
                            num_epochs = log["ctx"]["num_epochs"]

    except WaitTimeExceeded:
        if wait == 0:
            _stderr_print(
                "INFO: Parameter wait=0 was specified, not waiting for the job completion."
            )
        else:
            _stderr_print(
                f"WARN: Job hasn't completed after waiting for {wait} seconds. Exiting the script, but the job will remain running until it reaches the end state."
            )


def _get_quiet_poll_context(job: Job) -> Tuple[Optional[int], Optional[int]]:
    num_epochs = None
    num_records = None
    if isinstance(job, RecordHandler) and job.params:
        num_records = job.params.get("num_records")
    elif isinstance(job, Model):
        if job.model_type == "amplify":
            num_records = job.model_config["models"][0][job.model_type]["params"][
                "num_records"
            ]
        elif job.model_type in ["actgan", "gpt_x", "synthetics"]:
            num_epochs = job.model_config["models"][0][job.model_type]["params"][
                "epochs"
            ]
            num_records = job.model_config["models"][0][job.model_type]["generate"][
                "num_records"
            ]
        elif job.model_type == "navigator_ft":
            num_epochs = 1
            num_records = job.model_config["models"][0][job.model_type]["generate"][
                "num_records"
            ]
        # Fetch the ``num_epochs`` value from the logs in `pre` stage for `auto` value for `num_epochs`,
        if isinstance(num_epochs, str):
            num_epochs = None
    return num_epochs, num_records


def poll(job: Job, wait: int = WAIT_UNTIL_DONE, verbose: bool = True) -> None:
    """
        Polls a ``Model`` or ``RecordHandler``.

    Args:
        job: The job to poll.
        wait: The time to wait for the job to complete.
        verbose: ``False`` uses new quiet polling, defaults to ``True``.
    """
    if verbose:
        _verbose_poll(job, wait)
    else:
        num_epochs, num_records = _get_quiet_poll_context(job)
        _quiet_poll(job, wait, num_epochs, num_records)


def get_description_set(job: Job) -> Optional[dict]:
    model_type_config = get_model_type_config(job.model_type)
    if isinstance(job, Model):
        return model_type_config.train_status_descriptions
    if isinstance(job, RecordHandler):
        return model_type_config.run_status_descriptions


def do_api_call(
    method: str,
    path: str,
    query_params: Optional[dict] = None,
    body: Optional[dict] = None,
    headers: Optional[dict] = None,
    *,
    session: Optional[ClientConfig] = None,
) -> dict:
    """
    Make a direct API call to Gretel Cloud.

    Args:
        method: "get", "post", etc
        path: The full path to make the request to, any path params must be already included.
            Example: "/users/me"
        query_params: Optional URL based query parameters
        body: An optional JSON payload to send
        headers: Any custom headers that need to bet set.
        session: the session to use, or ``None`` to use the default session.

    NOTE:
        This function will automatically inject the appropriate API hostname and
        authentication from the Gretel configuration.
    """
    if session is None:
        session = get_session_config()

    if headers is None:
        headers = {}

    method = method.upper()

    if not path.startswith("/"):
        path = "/" + path

    api = session._get_api_client()

    # Utilize the ApiClient method to inject the proper authentication
    # into our headers, since Gretel only uses header-based auth we don't
    # need to pass any other data into this
    #
    # NOTE: This function does a pointer-like update of ``headers``
    api.update_params_for_auth(
        headers, None, api.configuration.auth_settings(), None, None, None
    )

    url = api.configuration.host + path

    response = api.request(
        method, url, query_params=query_params, body=body, headers=headers
    )

    resp_dict = json.loads(response.data.decode())
    return resp_dict.get("data")
