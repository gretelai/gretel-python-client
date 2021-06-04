import json
from typing import Dict, Optional, Union

import click

from gretel_client_v2._cli.common import (
    SessionContext,
    StatusDescriptions,
    get_status_description,
    record_transform_status_descriptions,
    record_generation_status_descriptions,
    model_option,
    project_option,
    pass_session,
    runner_option,
)
from gretel_client_v2.config import RunnerMode
from gretel_client_v2.projects.docker import ContainerRun


@click.group(help="Commands for working with records and running models.")
def records():
    ...


def model_path_option(fn):
    return click.option(
        "--model-path",
        metavar="PATH",
        help="Specify a remote path to the model.",
    )(fn)


def input_data_option(fn):
    return click.option(
        "--in-data",
        metavar="PATH",
        help="Specify model input data.",
    )(fn)


def output_data_option(fn):
    return click.option(
        "--output",
        metavar="DIR",
        help="Specify model output directory.",
    )(fn)


def create_and_run_record_handler(
    sc: SessionContext,
    params: Optional[dict],
    action: str,
    runner: str,
    output: str,
    in_data: str,
    model_path: Optional[str],
    data_source: Optional[str],
    status_strings: StatusDescriptions
):
    sc.log.info(f"Creating record handler for model {sc.model.model_id}")
    record_handler = sc.model.create_record_handler()

    try:
        data = record_handler.create(
            params=params, action=action, runner=runner, data_source=data_source
        )
        sc.register_cleanup(lambda: record_handler.cancel())
        sc.log.info(f"Record handler created {record_handler.record_id}")
        sc.print(data=data.get("data").get("handler"))
    except Exception as ex:
        sc.log.error("Could not create record handler", ex=ex)
        sc.exit(1)

    if runner == RunnerMode.LOCAL.value:
        run = ContainerRun.from_record_handler(record_handler)
        if output:
            run.configure_output_dir(output)
        if in_data:
            run.configure_input_data(in_data)
        if model_path:
            run.configure_model(model_path)
        run.start()
        sc.register_cleanup(lambda: run.graceful_shutdown())

    # Poll for the latest container status
    for update in record_handler.poll_logs_status():
        if update.transitioned:
            sc.log.info(
                (
                    f"Status is {update.status}. "
                    f"{get_status_description(status_strings, update.status, runner)}"
                )
            )
        for log in update.logs:
            msg = f"{log['ts']}  {log['msg']}"
            if log["ctx"]:
                msg += f"\n{json.dumps(log['ctx'], indent=4)}"
            click.echo(msg, err=True)
        if update.error:
            sc.log.error(f"\t{update.error}")

    if record_handler.status == "completed":
        sc.log.info(f"Done. Record {action} command done!")
    else:
        sc.log.error("The record command failed with the following error")
        sc.log.error(record_handler.errors)
        sc.log.error(
            f"Status is {record_handler.status}. Please scroll back through the logs for more details."
        )
        sc.exit(1)


@records.command(help="Generate synthetic records from a model.")
@project_option
@runner_option
@model_option
@model_path_option
@output_data_option
@input_data_option
@click.option("--num-records", help="Number of records to generate.", default=500)
@click.option(
    "--max-invalid",
    help="Number of invalid records generated before failure.",
    default=500,
)
@pass_session
def generate(
    sc: SessionContext,
    project: str,
    runner: str,
    output: str,
    in_data: str,
    model_path: str,
    model_id: str,
    num_records: int,
    max_invalid: int,
):
    if runner == RunnerMode.LOCAL.value and not output:
        raise click.BadOptionUsage(
            "--output",
            "--runner is set to local, but no --output flag is set. Please set an output path",
        )

    if runner == RunnerMode.LOCAL.value and sc.model.is_cloud_model and model_path:
        raise click.BadOptionUsage(
            "--model-path", "Cannot specify a local model path for cloud models"
        )

    params: Dict[str, Union[int, float, str]] = {
        "num_records": num_records,
        "max_invalid": max_invalid,
    }
    data_source = None
    if in_data and runner == RunnerMode.CLOUD.value:
        try:
            sc.log.info(f"Upload input artifact {in_data}")
            data_source = sc.project.upload_artifact(in_data)
        except Exception as ex:
            sc.log.error(f"Could not upload artifact {in_data}", ex=ex)
            sc.exit(1)

    create_and_run_record_handler(
        sc,
        params=params,
        data_source=data_source,
        action="generate",
        runner=runner,
        output=output,
        in_data=in_data,
        status_strings=record_generation_status_descriptions,
        model_path=model_path
    )


@records.command(help="Transform records via pipelines.")
@project_option
@runner_option
@model_option
@model_path_option
@input_data_option
@output_data_option
@pass_session
def transform(
    sc: SessionContext,
    project: str,
    model_path: str,
    in_data: str,
    output: str,
    runner: str,
    model_id: str,
):
    if runner == RunnerMode.LOCAL.value and not output:
        raise click.BadOptionUsage(
            "--output",
            "--runner is set to local, but no --output flag is set. Please set an output path",
        )

    if runner == RunnerMode.LOCAL.value and sc.model.is_cloud_model and model_path:
        raise click.BadOptionUsage(
            "--model-path", "Cannot specify a local model path for cloud models"
        )

    data_source = "__local__"
    if in_data and runner == RunnerMode.CLOUD.value:
        try:
            sc.log.info(f"Uploading input artifact {in_data}")
            data_source = sc.project.upload_artifact(in_data)
        except Exception as ex:
            sc.log.error(f"Could not upload artifact {in_data}", ex=ex)
            sc.exit(1)

    create_and_run_record_handler(
        sc,
        params=None,
        data_source=data_source,
        action="transform",
        runner=runner,
        output=output,
        in_data=in_data,
        status_strings=record_transform_status_descriptions,
        model_path=model_path
    )
