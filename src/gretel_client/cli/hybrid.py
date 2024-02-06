from typing import Optional

import click

from gretel_client.cli.common import pass_session, SessionContext
from gretel_client.config import get_session_config
from gretel_client.rest_v1.api.clusters_api import ClustersApi
from gretel_client.rest_v1.models import Cluster


@click.group(
    help="Commands for working with Gretel Hybrid features.",
)
def hybrid():
    ...


@hybrid.group(help="Commands for working with Gretel Hybrid environments.")
def environments():
    ...


def get_clusters_api() -> ClustersApi:
    return get_session_config().get_v1_api(ClustersApi)


def _format_clusters(clusters: list[Cluster], echo=click.echo):
    for cluster in clusters:
        _format_cluster(cluster, echo=echo)


def _format_cluster(cluster: Cluster, echo=click.echo):
    echo(f"{click.style(cluster.name, bold=True)} ({cluster.owner_profile.email})")
    echo(f"    ID:                    {cluster.guid}")
    cp_info = click.style("Unknown", fg="red")
    if cluster.cloud_provider:
        cp_type = None
        if cluster.cloud_provider.aws is not None:
            cp_type = "AWS"
        elif cluster.cloud_provider.gcp is not None:
            cp_type = "GCP"
        elif cluster.cloud_provider.azure is not None:
            cp_type = "Azure"
        if cp_type:
            cp_info = f"{cp_type}, {cluster.cloud_provider.region}"
    echo(f"    Cloud Provider/Region: {cp_info}")

    created_at = cluster.created_at.astimezone().strftime("%c")
    last_checkin_at = (
        cluster.last_checkin_time.astimezone().strftime("%c")
        if cluster.last_checkin_time
        else "unknown"
    )
    echo(f"    Created:               {created_at}")
    echo(f"    Last check-in:         {last_checkin_at}")


@environments.command(help="List hybrid environments.")
@click.option(
    "--owned-only",
    is_flag=True,
    default=False,
    help="Only show hybrid environments owned by you",
)
@pass_session
def list(
    sc: SessionContext,
    owned_only: Optional[bool] = None,
):
    clusters_api = get_clusters_api()
    clusters = clusters_api.list_clusters(
        owned_only=owned_only, expand=["owner"]
    ).clusters

    if not clusters:
        sc.log.info("No available hybrid environments")
        return

    sc.log.info("Available hybrid environments:")
    sc.print(data=clusters, auto_printer=_format_clusters)


@environments.command(help="Show information about a single hybrid environment.")
@click.argument("environment-name-or-id")
@pass_session
def get(
    sc: SessionContext,
    environment_name_or_id: str,
):
    clusters_api = get_clusters_api()
    clusters = [
        c
        for c in clusters_api.list_clusters(owned_only=False, expand=["owner"]).clusters
        if environment_name_or_id in (c.name, c.guid)
    ]
    if len(clusters) == 0:
        sc.log.error(f"No such hybrid environment: '{environment_name_or_id}'")
        sc.exit(1)
    if len(clusters) > 1:
        sc.log.error(f"The name '{environment_name_or_id}' is ambiguous:")
        sc.log.error(" ")
        _format_clusters(clusters, echo=sc.log.error)
        sc.log.error(" ")
        sc.log.error("Please specify the hybrid environment ID")
        sc.exit(1)

    sc.print(data=clusters[0], auto_printer=_format_cluster)
