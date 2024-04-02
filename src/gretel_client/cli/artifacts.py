import click

from gretel_client.cli.common import pass_session, project_option, SessionContext


@click.group(help="Commands for interacting with Gretel Cloud artifacts.")
def artifacts(): ...


@artifacts.command(help="List project or model artifacts.")
@click.option("--model-id", metavar="UID", help="Model id to list artifacts for.")
@project_option
@pass_session
def list(sc: SessionContext, model_id: str, project: str):
    artifact_listing = None
    if model_id:
        model = sc.project.get_model(model_id)
        sc.log.info(
            (
                f"Fetching artifact listing for model {model.model_id} "
                f"(project {sc.project.name})."
            )
        )
        artifact_listing = [
            {"artifact_type": art, "signed_url": link}
            for art, link in model.get_artifacts()
        ]
    else:
        sc.log.info(f"Fetching artifact listing for project {sc.project.name}.")
        artifact_listing = sc.project.artifacts

    if artifact_listing:
        sc.print(data=artifact_listing)
        sc.log.info(f"Got {len(artifact_listing)} artifacts.")
    else:
        sc.log.info("No artifacts found.")


@artifacts.command(help="Delete a project artifact.")
@click.option(
    "--artifact-key", metavar="KEY", help="The key of the project artifact to delete."
)
@click.option(
    "--all", default=False, is_flag=True, help="Delete all project artifacts."
)
@project_option
@pass_session
def delete(sc: SessionContext, artifact_key: str, project: str, all: bool):
    if artifact_key and all:
        raise click.BadOptionUsage("--all", "Cannot pass --artifact-key with --all.")

    if all:
        sc.log.info(f"Deleting all artifacts from project {sc.project.name}.")
        deleted_artifacts = 0
        for artifact in sc.project.artifacts:
            sc.log.info(f"Deleting artifact {artifact['key']}.")
            sc.project.delete_artifact(artifact["key"])
            deleted_artifacts += 1
        sc.log.info(f"Deleted {deleted_artifacts} artifacts.")
        sc.exit(0)

    sc.log.info(f"Deleting artifact {artifact_key}.")
    sc.project.delete_artifact(artifact_key)
    sc.log.info("Artifact deleted.")


@artifacts.command(help="Upload a project artifact.")
@click.option("--in-data", metavar="PATH", help="Path to the dataset to upload.")
@project_option
@pass_session
def upload(sc: SessionContext, in_data: str, project: str):
    sc.log.info(f'Uploading artifact from "{in_data}".')
    artifact = None
    artifact = sc.project.upload_artifact(in_data)

    if artifact:
        sc.log.info(
            (
                "Artifact uploaded. You may reference this key your project models"
                f"\n\n\t{artifact}\n"
            )
        )
        sc.log.info("Done.")
