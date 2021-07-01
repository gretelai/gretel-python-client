import json
from typing import Dict, Optional, Union

import click

from gretel_client.cli.common import (
    SessionContext,
    StatusDescriptions,
    download_artifacts,
    model_option,
    pass_session,
    poll_and_print,
    project_option,
    record_classify_status_descriptions,
    record_generation_status_descriptions,
    record_transform_status_descriptions,
    runner_option,
)
from gretel_client.config import RunnerMode
from gretel_client.projects.docker import ContainerRun
from gretel_client.projects.jobs import Status


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
    def callback(ctx, param: click.Option, value: str):
        gc: SessionContext = ctx.ensure_object(SessionContext)
        return value or gc.data_source

    return click.option(  # type: ignore
        "--in-data",
        metavar="PATH",
        callback=callback,
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
    status_strings: StatusDescriptions,
):
    sc.log.info(f"Creating record handler for model {sc.model.model_id}")
    record_handler = sc.model.create_record_handler()

    try:
        data = record_handler.create(
            params=params, action=action, runner_mode=RunnerMode(runner), data_source=data_source
        )
        sc.register_cleanup(lambda: record_handler.cancel())
        sc.log.info(f"Record handler created {record_handler.record_id}")

        if runner == RunnerMode.MANUAL.value:
            # With --runner MANUAL, we only print the worker_key and it's up to the user to run the worker
            sc.print(data={
                "record_handler": data,
                "worker_key": record_handler.worker_key
            })
        else:
            sc.print(data=data)
    except Exception as ex:
        sc.log.error("Could not create record handler", ex=ex)
        sc.exit(1)

    run = None
    if runner == RunnerMode.LOCAL.value:
        run = ContainerRun.from_job(record_handler)
        if sc.debug:
            sc.log.debug("Enabling debug for the container run")
            run.enable_debug()
        if output:
            run.configure_output_dir(output)
        if in_data:
            run.configure_input_data(in_data)
        if model_path:
            run.configure_model(model_path)
        run.start()
        sc.register_cleanup(lambda: run.graceful_shutdown())

    # Poll for the latest container status
    poll_and_print(
        record_handler, sc, runner, status_strings, callback=run.is_ok if run else None
    )

    if output and runner == RunnerMode.CLOUD.value:
        download_artifacts(sc, output, record_handler)

    if output and run:
        sc.log.info("Extracting record artifacts from container")
        run.extract_output_dir(output)

    if record_handler.status == Status.COMPLETED:
        sc.log.info(
            (
                "Billing estimate"
                f"\n{json.dumps(record_handler.billing_details, indent=4)}"
            )
        )
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

    if runner == RunnerMode.MANUAL.value and (output or in_data or model_path):
        raise click.BadOptionUsage(
            "--runner",
            "--runner manual cannot be used together with any of --output, --in-data, --model-path."
        )

    params: Dict[str, Union[int, float, str]] = {
        "num_records": num_records,
        "max_invalid": max_invalid,
    }
    data_source = None
    if in_data and runner == RunnerMode.CLOUD.value:
        try:
            sc.log.info(f"Uploading input artifact {in_data}")
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
        model_path=model_path,
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

    if runner == RunnerMode.MANUAL.value and (output or in_data or model_path):
        raise click.BadOptionUsage(
            "--runner",
            "--runner manual cannot be used together with any of --output, --in-data, --model-path."
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
        model_path=model_path,
    )


@records.command(help="Classify records.")
@project_option
@runner_option
@model_path_option
@input_data_option
@output_data_option
@model_option
@pass_session
def classify(
    sc: SessionContext,
    project: str,
    in_data: str,
    output: str,
    runner: str,
    model_id: str,
    model_path: str
):
    if runner == RunnerMode.LOCAL.value and not output:
        raise click.BadOptionUsage(
            "--output",
            "--runner is set to local, but no --output flag is set. Please set an output path",
        )

    if runner == RunnerMode.MANUAL.value and (output or in_data):
        raise click.BadOptionUsage(
            "--runner",
            "--runner manual cannot be used together with any of --output, --in-data."
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
        action="classify",
        runner=runner,
        output=output,
        in_data=in_data,
        status_strings=record_classify_status_descriptions,
        model_path=model_path,
    )
