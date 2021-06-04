import json
from pathlib import Path
from urllib.parse import urlparse

import click
import requests

from gretel_client_v2._cli.common import (
    SessionContext,
    get_status_description,
    model_create_status_descriptions,
    pass_session,
    project_option,
    runner_option,
)
from gretel_client_v2.projects.docker import (
    ContainerRun,
    ContainerRunError
)
from gretel_client_v2.projects.models import (
    Model,
    ModelArtifactError,
    ModelError,
    RunnerMode,
)
from gretel_client_v2.rest.exceptions import ApiException, NotFoundException


def _download_artifacts(sc: SessionContext, output: str, model: Model):
    output_path = Path(output)
    output_path.mkdir(exist_ok=True, parents=True)
    sc.log.info(f"Downloading model artifacts to {output_path.resolve()}")
    for type, download_link in model.get_artifacts():
        try:
            art = requests.get(download_link)
            if art.status_code == 200:
                art_output_path = output_path / Path(urlparse(download_link).path).name
                with open(art_output_path, "wb+") as out:
                    sc.log.info(f"\tWriting {type} to {art_output_path}")
                    out.write(art.content)
        except requests.exceptions.HTTPError:
            sc.log.info(f"\tSkipping {type}. Artifact not found.")


@click.group(help="Commands for training and working with models.]")
def models():
    ...


@models.command()
@click.option(
    "--config",
    metavar="PATH",
    help="Path to Gretel Config file. This can be a local or remote file path.",
    required=True,
)
@click.option(
    "--wait",
    metavar="MINUTES",
    help="Configures the time in minutes to wait for a model to finish running.",
    default=0,
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
    help="If set, and --runner is set to local, model results will be uploaded to Gretel Cloud. When using the cloud runner, results will always be uploaded to Gretel Cloud.",
)
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="If set, the model config will be validated against Gretel APIs, but no work will be scheduled to run.",
)
@project_option
@runner_option
@pass_session
def create(
    sc: SessionContext,
    config: str,
    wait: int,
    in_data: str,
    output: str,
    runner: str,
    upload_model: bool,
    project: str,
    dry_run: bool,
):
    if wait > 0 and output:
        raise click.BadOptionUsage(
            "--output",
            "An output dir is specified but --wait is > 0. Please re-run with --wait=0.",
        )

    if runner == RunnerMode.LOCAL.value and not output:
        raise click.BadOptionUsage(
            "--output",
            "--runner is set to local, but --output is not set. Please specify an output directory for --output.",
        )

    sc.log.info("Preparing model")
    model: Model = sc.project.create_model(config)

    # Figure out if we need to upload a data source as an artifact. By
    # default any cloud run will require an external data source to
    # be uploaded.
    if in_data:
        model.data_source = in_data

    try:
        model.validate_data_source()
    except ModelArtifactError as ex:
        sc.log.error(f"The data source '{model.data_source}' is not valid", ex=ex)
        sc.exit(1)

    if runner == RunnerMode.CLOUD.value:
        sc.log.info("Uploading input data source")
        key = model.upload_data_source(
            _validate=False
        )  # the data source was validated in a previous step
        sc.log.info("Data source uploaded")
        sc.log.info(
            (
                "Gretel artifact key is"
                f"\n\n\t{key}\n\n"
                f"You can re-use this key for any model in the project {sc.project.name}"
            )
        )

    # Create the model and the data source
    try:
        sc.log.info("Creating model")
        run = model.submit(
            runner_mode=RunnerMode(runner), dry_run=dry_run, _validate_data_source=False
        )
        sc.register_cleanup(lambda: model.cancel())
        sc.log.info("Model created")
        del run["model_key"]
        sc.print(data=run)
    except ApiException as ex:
        sc.print(data=json.loads(ex.body))  # type:ignore
        sc.exit(1)
    except Exception as ex:
        sc.log.error("Could not load model", ex=ex)
        sc.exit(1)

    if dry_run:
        sc.exit(0)

    # Start a local container when --runner is manual
    if runner == RunnerMode.LOCAL.value:
        run = ContainerRun.from_model(model)
        if output:
            run.configure_output_dir(output)
        if in_data:
            run.configure_input_data(in_data)
        if upload_model:
            run.enable_cloud_uploads()
        if model.model_type == "synthetics":
            sc.log.info("Configuring GPU for model training")
            try:
                run.configure_gpu()
                sc.log.info("GPU device found!")
            except ContainerRunError:
                sc.log.warn("Could not configure GPU. Continuing with CPU")
        sc.log.info(
            f"This model is configured to run locally using the container {run.image}"
        )
        sc.log.info("Pulling the container and starting local model training.")
        run.start()
        sc.register_cleanup(lambda: run.graceful_shutdown())

    # Poll for the latest container status
    for update in model.poll_logs_status(wait):
        if update.transitioned:
            sc.log.info(
                (
                    f"Status is {update.status}. "
                    f"{get_status_description(model_create_status_descriptions, update.status, runner)}"
                )
            )
        for log in update.logs:
            msg = f"{log['ts']}  {log['msg']}"
            if log["ctx"]:
                msg += f"\n{json.dumps(log['ctx'], indent=4)}"
            click.echo(msg, err=True)
        if update.error:
            sc.log.error(f"\t{update.error}")

    # If the job is in the cloud, and --output is passed, we
    # want to download the artifacts. This isn't necessary for
    # local runs since there is already a volume mount.
    if output and runner == RunnerMode.CLOUD.value and wait == 0:
        _download_artifacts(sc, output, model)

    sc.print(data=model._data.get("model"))
    sc.log.info(
        (
            "Billing estimate"
            f"\n{json.dumps(model._data.get('billing_estimate'), indent=4)}"
        )
    )
    sc.log.info("Note: no charges will be incurred during the beta period")

    if model.status == "completed":
        sc.log.info("Done. Model created!")
    else:
        sc.log.error("The model failed with the following error")
        sc.log.error(model.errors, ex=model.traceback)
        sc.log.error(
            f"Status is {model.status}. Please scroll back through the logs for more details."
        )
        sc.exit(1)


@models.command()
@click.option("--model-id", metavar="UID")
@click.option(
    "--output",
    metavar="DIR",
    help="Specify output directory to download model artifacts to.",
)
@project_option
@pass_session
def get(sc: SessionContext, project: str, model_id: str, output: str):
    model: Model = sc.project.get_model(model_id)
    sc.print(data=model._data)
    if output:
        if model.status != "completed":
            sc.log.error(
                f"""
                Cannot download model artifacts. Model should be in a completed
                state, but is instead {model.status}"""
            )
            sc.exit(1)
        _download_artifacts(sc, output, model)
    sc.log.info("Done fetching model.")


@models.command()
@project_option
@pass_session
def search(sc: SessionContext, project: str):
    sc.print(data=sc.project.search_models())


@models.command()
@click.option("--model-id", metavar="UID")
@project_option
@pass_session
def delete(sc: SessionContext, project: str, model_id: str):
    sc.log.info(f"Deleting model {model_id}")
    try:
        model: Model = sc.project.get_model(model_id)
        model.delete()
    except (ModelError, NotFoundException) as ex:
        sc.log.error(
            "Could not delete model. Check you have the right model id.",
            include_tb=True,
            ex=ex,
        )
        sc.exit(1)
    sc.log.info("Model deleted.")
