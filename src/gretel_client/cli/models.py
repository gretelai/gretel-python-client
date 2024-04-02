import json

from pathlib import Path
from typing import Tuple, Union

import click

from gretel_client.cli.common import (
    model_option,
    pass_session,
    poll_and_print,
    project_option,
    ref_data_option,
    runner_option,
    SessionContext,
)
from gretel_client.cli.utils.parser_utils import ref_data_factory, RefData
from gretel_client.models.config import get_model_type_config, GPU
from gretel_client.projects.common import ModelArtifact, WAIT_UNTIL_DONE
from gretel_client.projects.docker import ContainerRun, ContainerRunError
from gretel_client.projects.jobs import Status
from gretel_client.projects.models import Model, RunnerMode


@click.group(help="Commands for training and working with models.")
def models(): ...


@models.command(help="Create a new model.")
@click.option(
    "--config",
    metavar="PATH",
    help="Specify the path to Gretel Config file. It can be a local or remote file path.",
    required=True,
)
@click.option(
    "--name",
    metavar="MODEL",
    help="Specify a name for the model.",
    required=False,
)
@click.option(
    "--wait",
    metavar="SECONDS",
    help="Configure the time in seconds to wait for a model to finish running.",
    default=WAIT_UNTIL_DONE,
)
@click.option(
    "--output",
    metavar="DIR",
    help="Specify an output directory to place model artifacts.",
)
@click.option(
    "--in-data", metavar="PATH", help="Specify an input file to train the model with."
)
@click.option(
    "--upload-model",
    default=False,
    is_flag=True,
    help="If set, and --runner is set to local, model results will be uploaded to Gretel Cloud. When using the cloud runner, results will always be uploaded to Gretel Cloud.",  # noqa
)
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="If set, the model config will be validated against Gretel APIs, but no work will be scheduled to run.",
)
@project_option
@ref_data_option
@runner_option
@pass_session
def create(
    sc: SessionContext,
    config: str,
    wait: int,
    in_data: str,
    ref_data: tuple[str, ...],
    output: str,
    runner: str,
    upload_model: bool,
    project: str,
    dry_run: bool,
    name: str,
):
    if wait >= 0 and output:
        raise click.BadOptionUsage(
            "--output",
            "An output dir is specified but --wait is >= 0. "
            "Please re-run without --wait argument (will wait until the job is done).",
        )

    runner = sc.runner
    if runner == RunnerMode.LOCAL.value and not output:
        raise click.BadOptionUsage(
            "--output",
            "--runner is set to local, but --output is not set. Please specify an output directory for --output.",
        )

    if runner in (RunnerMode.MANUAL.value, RunnerMode.HYBRID.value) and (
        output or upload_model
    ):
        raise click.BadOptionUsage(
            "--runner",
            f"--runner {runner} cannot be used together with any of --output, --upload-model.",
        )

    ref_data_obj = ref_data_factory(ref_data)

    sc.log.info("Preparing model.")
    model: Model = sc.project.create_model_obj(config)

    if name:
        model.name = name

    # Figure out if we need to upload a data source as an artifact. By
    # default any cloud run will require an external data source to
    # be uploaded.
    if in_data:
        model.data_source = in_data

    if not ref_data_obj.is_empty:
        model.ref_data = ref_data_obj

    if runner != RunnerMode.MANUAL.value:
        model.validate_data_source()
        model.validate_ref_data()

    if runner == RunnerMode.CLOUD.value:
        sc.log.info("Uploading input data source...")
        key = model.upload_data_source(
            _validate=False
        )  # the data source was validated in a previous step
        sc.log.info("Data source uploaded.")
        sc.log.info(
            (
                "Gretel artifact key is"
                f"\n\n\t{key}\n\n"
                f"You can re-use this key for any model in the project {sc.project.name}."
            )
        )

        if not ref_data_obj.is_empty:
            sc.log.info("Uploading ref data sources...")
            uploaded_ref_data = model.upload_ref_data(_validate=False)
            sc.log.info("Ref data uploaded.")
            sc.log.info("Gretel artifact keys for ref data are:")
            for ref_data_source in uploaded_ref_data.values:
                sc.log.info(f"\t{ref_data_source}")
            sc.log.info(
                f"You can re-use these keys for any model in the project {sc.project.name}."
            )

    # Create the model and the data source
    sc.log.info("Creating model.")
    run = model.submit(
        runner_mode=RunnerMode.parse(runner),
        dry_run=dry_run,
    )
    sc.register_cleanup(lambda: model.cancel())
    sc.log.info(f"Model created with ID {model.model_id}.")

    if runner == RunnerMode.MANUAL.value:
        # With --runner MANUAL, we only print the worker_key and it's up to the user to run the worker
        sc.print(data={"model": run.print_obj, "worker_key": model.worker_key})
    else:
        sc.log.info(data=run.print_obj)

    if dry_run:
        sc.exit(0)

    # Start a local container when --runner is LOCAL
    #
    # The submit call above will have triggered the model to have been created in manual mode
    # so at this point the model instance is hydrated with the data from the Cloud API
    run = None
    if runner == RunnerMode.LOCAL.value:
        run = ContainerRun.from_job(model)
        if sc.debug:
            sc.log.debug("Enabling debug logs for the local container.")
            run.enable_debug()
        if output:
            run.configure_output_dir(output)
        if model.external_data_source:
            run.configure_input_data(model.data_source)
        if model.external_ref_data:
            run.configure_ref_data(model.ref_data)
        if upload_model:
            sc.log.info("Uploads to Gretel Cloud are enabled.")
            run.enable_cloud_uploads()
        if model.instance_type == GPU:
            sc.log.info("Configuring GPU for model training.")
            try:
                run.configure_gpu()
                sc.log.info("GPU device found!")
            except ContainerRunError:
                sc.log.warn("Could not configure GPU. Continuing with CPU.")
        sc.log.info(
            f"This model is configured to run locally using the container {run.image}."
        )
        sc.log.info("Pulling the container and starting local model training.")
        run.start()
        sc.register_cleanup(lambda: run.graceful_shutdown())  # type:ignore

    # Poll for the latest container status
    poll_and_print(
        model,
        sc,
        runner,
        get_model_type_config(model.model_type).train_status_descriptions,
        callback=run.is_ok if run else None,
        wait=wait,
    )

    # If the job is in the cloud, and --output is passed, we
    # want to download the artifacts. This isn't necessary for
    # local runs since there is already a volume mount.
    if output and runner == RunnerMode.CLOUD.value and wait == WAIT_UNTIL_DONE:
        model.download_artifacts(output)

    if output and runner == RunnerMode.LOCAL.value and run:
        sc.log.info(f"Extracting run output into {output}.")
        run.extract_output_dir(output)

    report_path = None
    report_data = None

    if output:
        report_path = Path(output) / f"{ModelArtifact.REPORT_JSON}.json.gz"

    sc.print(data=model.print_obj)
    sc.log.info("Fetching model report...")

    if report_path is None:
        report_path = model.get_artifact_link("report_json")
    else:
        report_path = str(report_path)

    if report_path is None:
        # Log the problem but keep going so we print the "download here" message below.
        sc.log.info("Unable to get report path. Cannot display preview.")
    else:
        report_data = model.peek_report(report_path)

    if report_data:
        sc.log.info(f"{json.dumps(report_data, indent=4)}")
    else:
        sc.log.info("Report is empty or could not be parsed.")

    if output:
        sc.log.info(
            "For a more detailed view of the report, please refer to the "
            f"full report artifact found under the output directory: `{output}`."
        )
    else:
        sc.log.info(
            (
                "For a more detailed view, you can download the HTML report using the CLI command \n\n"
                f"\tgretel models get --project {sc.project.name} --model-id {model.model_id} --output .\n"
            )
        )
    sc.log.info(("Billing estimate" f"\n{json.dumps(model.billing_details, indent=4)}"))

    if model.status == Status.COMPLETED:
        sc.log.info(
            (
                f"Model done training. The model id is\n\n\t{model.model_id}\n\n"
                f"You can re-use this key for gretel models run [...] commands in the project {sc.project.name}."
            )
        )
        sc.log.info("Done.")
    else:
        sc.log.error("The model failed with the following error.")
        sc.log.error(model.errors, ex=model.traceback, include_tb=False)
        sc.log.error(
            f"Status is {model.status}. Please scroll back through the logs for more details."
        )
        sc.exit(1)


@models.command(help="Download all model associated artifacts.")
@model_option
@click.option(
    "--output",
    metavar="DIR",
    help="Specify the output directory to download model artifacts to.",
    default=".",
)
@project_option
@pass_session
def get(sc: SessionContext, project: str, model_id: dict, output: str):
    model: Model = sc.project.get_model(model_id["uid"])
    sc.print(data=model.print_obj)
    if output:
        if model.status != "completed":
            sc.log.error(
                f"""
                Cannot download model artifacts. Model should be in a completed
                state, but is instead {model.status}."""
            )
            sc.exit(1)
        model.download_artifacts(output)
    sc.log.info("Done fetching model.")


@models.command(help="Search for models of the project.")
@project_option
@click.option("--limit", help="Limit the number of projects.", default=100)
@click.option("--model-name", help="Model name to match on", default="")
@pass_session
def search(sc: SessionContext, project: str, limit: int, model_name: str):
    sc.print(
        data=list(
            sc.project.search_models(factory=dict, limit=limit, model_name=model_name)
        )
    )


@models.command(help="Delete model.")
@model_option
@project_option
@pass_session
def delete(sc: SessionContext, project: str, model_id: dict):
    sc.log.info(f"Deleting model {model_id['uid']}.")
    model: Model = sc.project.get_model(model_id["uid"])
    model.delete()
    sc.log.info("Model deleted.")
