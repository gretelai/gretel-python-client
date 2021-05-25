import json
from pathlib import Path
from urllib.parse import urlparse

import click
import requests

from gretel_client_v2._cli.common import SessionContext, pass_session, project_option
from gretel_client_v2.projects.docker import ContainerRun
from gretel_client_v2.projects.models import (
    ArtifactError,
    Model,
    RunnerMode,
    ModelError,
)
from gretel_client_v2.rest.exceptions import ApiException, NotFoundException
from gretel_client_v2.config import DEFAULT_RUNNER, get_session_config


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


@click.group()
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
    "--runner",
    metavar="TYPE",
    type=click.Choice([m.value for m in RunnerMode], case_sensitive=False),
    default=lambda: get_session_config().default_runner,
    show_default="from ~/.gretel/config.json",
    help="Determines where to schedule the model run.",
)
@click.option(
    "--upload-model",
    default=True,
    is_flag=True,
    help="If set to `false` no model data will be uploaded from the local training run. This may only be set to `false` if --runner is set to local",
)
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="If set, the model config will be validated against Gretel APIs, but no work will be scheduled to run.",
)
@project_option
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

    if not upload_model and runner == "cloud":
        raise click.BadOptionUsage(
            "--upload-model", "Cannot disable model uploads for cloud runs."
        )

    model: Model = sc.project.create_model(config)

    # Figure out if we need to upload a data source as an artifact. By
    # default any cloud run will require an external data source to
    # be uploaded.
    upload_data_source = False
    if in_data:
        model.data_source = in_data
        if runner == RunnerMode.CLOUD.value:
            sc.log.info("Preparing to upload input data source")
            upload_data_source = True

    # Create the model and the data source
    try:
        sc.log.info("Creating model...")
        run = model.submit(
            runner_mode=RunnerMode(runner),
            dry_run=dry_run,
            upload_data_source=upload_data_source,
        )
        sc.log.info("Model created")
        sc.print(data=run)
    except ApiException as ex:
        sc.print(data=json.loads(ex.body))
        sc.exit(1)
    except ArtifactError as ex:
        sc.log.error(
            f"There was a problem with the input data source {model.data_source}", ex
        )
        sc.exit(1)
    except Exception as ex:
        sc.log.error("Could not load model", ex)
        sc.exit(1)

    if dry_run:
        sc.exit(0)

    # Start a local container when --runner is manual
    if runner == RunnerMode.LOCAL.value:
        try:
            model.peek_data_source()
        except ArtifactError as ex:
            sc.log.error(f"The data source '{model.data_source}' is not valid", ex=ex)
            sc.exit(1)
        run = ContainerRun(model, output_dir=output, disable_uploads=not upload_model)
        sc.log.info(f"Starting local container using {run.image}")
        run.start()

    # Poll for the latest container status
    for update in model.poll_logs_status(wait):
        if update.transitioned:
            sc.log.info(f"Status is {update.status}")
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
    if (
        output
        and model.status == "completed"
        and runner == RunnerMode.CLOUD.value
        and wait == 0
    ):
        _download_artifacts(sc, output, model)

    sc.print(data=model._data.get("model"))
    sc.log.info("Done. Model created.")


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
