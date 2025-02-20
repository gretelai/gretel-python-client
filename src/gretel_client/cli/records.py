import json

from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import click
import yaml

from gretel_client.cli.common import (
    model_option,
    pass_session,
    poll_and_print,
    project_option,
    record_handler_option,
    ref_data_option,
    runner_option,
    SessionContext,
    StatusDescriptions,
)
from gretel_client.cli.models import models
from gretel_client.cli.utils.parser_utils import ref_data_factory, RefData
from gretel_client.config import RunnerMode
from gretel_client.models.config import get_model_type_config, GPU
from gretel_client.projects.jobs import Status
from gretel_client.projects.records import RecordHandler

LOCAL = "__local__"


@click.group(help="Commands for working with records and running models.")
def records(): ...


def model_path_option(fn):
    return click.option(
        "--model-path",
        metavar="PATH",
        help="Specify a remote path to the model.",
    )(fn)


def input_data_option(fn):
    return click.option(  # type: ignore
        "--in-data",
        metavar="PATH",
        help="Specify the model input data.",
    )(fn)


def output_data_option(fn):
    return click.option(
        "--output",
        metavar="DIR",
        help="Specify the model output directory.",
    )(fn)


def _validate_params(
    sc: SessionContext, runner: str, output: str, model_path: str, in_data: str
):
    if runner == RunnerMode.CLOUD.value and model_path:
        raise click.BadOptionUsage(
            "--model-path", "A model path may not be specified for cloud models."
        )
    if (
        not sc.model.is_cloud_model
        and runner == RunnerMode.LOCAL.value
        and not model_path
    ):
        raise click.BadOptionUsage(
            "--model-path", "--model-path is required when running a local model."
        )
    if runner == RunnerMode.LOCAL.value and not output:
        raise click.BadOptionUsage(
            "--output",
            "--runner is set to local, but no --output flag is set. Please set an output path.",
        )

    if runner == RunnerMode.LOCAL.value and sc.model.is_cloud_model and model_path:
        raise click.BadOptionUsage(
            "--model-path", "Cannot specify the local model path for cloud models."
        )

    if runner in (RunnerMode.MANUAL.value, RunnerMode.HYBRID.value) and (
        output or model_path
    ):
        raise click.BadOptionUsage(
            "--runner",
            f"--runner {runner} cannot be used together with any of --output, --model-path.",
        )


def _configure_data_source(
    sc: SessionContext, in_data: Optional[str], runner: str
) -> Optional[str]:
    # NOTE: If ``in_data`` is already None, we just return None which
    # will then be passed into the record handler API call. If the job
    # being requested does require a data source, then the API will reject
    # the API call. If the data source is optional, then the API call will succeed.
    #
    # If the job is local then the "__local__" string will be passed into the API
    # call and stored in the cloud config. This is pro forma in order to get the record
    # handler config validators to pass. When the job container actually starts, this
    # __local__ value will be replaced by the actual data source

    if in_data is None:
        return None

    if runner == RunnerMode.MANUAL.value:
        data_source = in_data
    elif runner == RunnerMode.CLOUD.value:
        sc.log.info(f"Uploading input artifact {in_data}")
        data_source = sc.project.upload_artifact(in_data)
    else:
        data_source = LOCAL

    return data_source


def _configure_ref_data(ref_data: RefData, runner: str) -> RefData:
    # If the job is being run locally, we swap the data sources to __local__
    # so we don't expose anything in the stored cloud config
    if runner == RunnerMode.LOCAL.value:
        for key, data_source in ref_data.ref_dict.items():
            ref_data.ref_dict[key] = LOCAL
    return ref_data


def create_and_run_record_handler(
    sc: SessionContext,
    *,
    params: Optional[dict],
    runner: str,
    output: str,
    in_data: Optional[str],
    model_path: Optional[str],
    data_source: Optional[str],
    status_strings: StatusDescriptions,
    ref_data: Optional[RefData] = None,
):

    # NOTE: ``in_data`` is only evaluated to determine if we should set a --data-source
    # for the CLI args that get passed into a local container.  The ``data_source`` value
    # is what will be sent to the Cloud API.

    sc.log.info(f"Creating record handler for model {sc.model.model_id}.")
    record_handler = sc.model.create_record_handler_obj(
        params=params,
        data_source=data_source,
        ref_data=ref_data,
    )

    data = record_handler.submit(runner_mode=RunnerMode.parse(runner))
    sc.register_cleanup(lambda: record_handler.cancel())
    sc.log.info(f"Record handler created {record_handler.record_id}.")

    printable_record_handler = data.print_obj
    if runner == RunnerMode.MANUAL.value:
        # With --runner MANUAL, we only print the worker_key and it's up to the user to run the worker
        sc.print(
            data={
                "record_handler": printable_record_handler,
                "worker_key": record_handler.worker_key,
            }
        )
    else:
        sc.print(data=printable_record_handler)

    run = None
    # Poll for the latest container status
    poll_and_print(record_handler, sc, runner, status_strings)

    if output and runner == RunnerMode.CLOUD.value:
        record_handler.download_artifacts(output)

    if output and run:
        sc.log.info("Extracting record artifacts from the container.")
        run.extract_output_dir(output)

    if record_handler.status == Status.COMPLETED:
        sc.log.info(
            (
                "For a more detailed view, you can download record artifacts using the CLI command \n\n"
                f"\tgretel records get --project {sc.project.name} --model-id {sc.model.model_id} --record-handler-id {record_handler.record_id} --output .\n"
            )
        )
        sc.log.info(
            (
                "Billing estimate"
                f"\n{json.dumps(record_handler.billing_details, indent=4)}."
            )
        )
        sc.log.info("Done.")
    else:
        sc.log.error("The record command failed with the following error.")
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
    default=None,
)
@pass_session
def generate(
    sc: SessionContext,
    project: str,
    runner: str,
    output: str,
    in_data: str,
    model_path: str,
    model_id: dict,
    num_records: int,
    max_invalid: int,
):
    runner = sc.runner
    _validate_params(sc, runner, output, model_path, in_data)

    data_source = _configure_data_source(sc, in_data, runner)

    params: Dict[str, Union[int, float, str]] = {
        "num_records": num_records,
        "max_invalid": max_invalid,
    }

    create_and_run_record_handler(
        sc,
        params=params,
        data_source=data_source,
        runner=runner,
        output=output,
        in_data=in_data,
        status_strings=get_model_type_config("synthetics").run_status_descriptions,
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
    runner = sc.runner
    _validate_params(sc, runner, output, model_path, in_data)

    data_source = _configure_data_source(sc, in_data, runner)

    create_and_run_record_handler(
        sc,
        params=None,
        data_source=data_source,
        runner=runner,
        output=output,
        in_data=in_data,
        status_strings=get_model_type_config("transform").run_status_descriptions,
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
    model_path: str,
):
    runner = sc.runner
    _validate_params(sc, runner, output, model_path, in_data)

    data_source = _configure_data_source(sc, in_data, runner)

    create_and_run_record_handler(
        sc,
        params=None,
        data_source=data_source,
        runner=runner,
        output=output,
        in_data=in_data,
        status_strings=get_model_type_config("classify").run_status_descriptions,
        model_path=model_path,
    )


@models.command(help="Run an existing model.")
@project_option
@runner_option
@model_path_option
@input_data_option
@ref_data_option
@output_data_option
@model_option
@pass_session
@click.option(
    "--param",
    type=(str, str),
    multiple=True,
    help="Specify parameters to pass into the record handler.",
)
@click.option(
    "--params-file",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, path_type=Path
    ),
    help="Specify a file (YAML or JSON) with all parameters to pass into the record handler.",
)
def run(
    sc: SessionContext,
    project: str,
    model_id: str,
    in_data: Optional[str],
    ref_data: tuple[str, ...],
    output: str,
    runner: str,
    model_path: str,
    param: tuple[tuple[str, str], ...],
    params_file: Optional[Path],
):
    """
    Generic run command.
    """

    extra_params = None
    if params_file is not None:
        with open(params_file, "r") as f:
            extra_params = yaml.safe_load(f)
    if param and len(param) > 0:
        if extra_params is not None:
            raise click.BadOptionUsage(
                "--param",
                "The --param option cannot be used in conjunction with --params-file.",
            )
        extra_params = {key: value for key, value in param}

    runner = sc.runner
    _validate_params(sc, runner, output, model_path, None)

    # The idea here:
    # - in_data is what the CLI argument was
    # - data_source is what is going to be sent to the API in the model config
    data_source = None
    if in_data:
        data_source = _configure_data_source(sc, in_data, runner)

    ref_data = ref_data_factory(ref_data)
    ref_data = _configure_ref_data(ref_data, runner)

    create_and_run_record_handler(
        sc,
        params=extra_params,
        in_data=in_data,
        data_source=data_source,
        ref_data=ref_data,
        runner=runner,
        output=output,
        status_strings=get_model_type_config().run_status_descriptions,
        model_path=model_path,
    )


@records.command(help="Download all record handler associated artifacts.")
@click.option(
    "--output",
    metavar="DIR",
    help="Specify the output directory to download record handler artifacts to.",
    default=".",
)
@project_option
@click.option(
    "--model-id",
    metavar="ID",
    help="Specify the model.",
    required=False,
)
@record_handler_option
@pass_session
def get(
    sc: SessionContext,
    record_handler_id: dict,
    model_id: Optional[str],
    project: str,
    output: str,
):
    rh_model_id = record_handler_id.get("model_id")
    if not rh_model_id and not model_id:
        raise click.BadOptionUsage("--model-id", "Please specify a model ID")
    if rh_model_id and model_id and rh_model_id != model_id:
        raise click.BadOptionUsage(
            "--model-id",
            "Explicitly specified model ID does not match model ID found in record handler JSON file",
        )

    model_id = model_id or rh_model_id

    record_handler: RecordHandler = sc.project.get_model(model_id).get_record_handler(
        record_handler_id["uid"]
    )
    if record_handler.status != "completed":
        sc.log.error(
            f"""
                Cannot download record handler artifacts. Record handler should be in a completed
                state, but is instead {record_handler.status}."""
        )
        sc.exit(1)
    record_handler.download_artifacts(output)
    sc.log.info("Done fetching record handler artifacts.")
