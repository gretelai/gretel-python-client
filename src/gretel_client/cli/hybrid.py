import re

from typing import Optional

import click

from gretel_client.cli.common import pass_session, SessionContext
from gretel_client.config import ClientConfig
from gretel_client.rest_v1.api.clusters_api import ClustersApi
from gretel_client.rest_v1.models import Cluster


class ClusterError(Exception):
    pass


@click.group(
    help="Commands for working with Gretel Hybrid features.",
)
def hybrid(): ...


@hybrid.group(help="Commands for working with Gretel Hybrid environments.")
def environments(): ...


def get_clusters_api(*, session: ClientConfig) -> ClustersApi:
    return session.get_v1_api(ClustersApi)


def _format_clusters(clusters: list[Cluster], echo=click.echo):
    for cluster in clusters:
        _format_cluster(cluster, echo=echo)


_HEALTH_STATUS_COLORS = {
    "HEALTH_STATUS_HEALTHY": "green",
    "HEALTH_STATUS_DEGRADED": "yellow",
    "HEALTH_STATUS_UNHEALTHY": "red",
}


def _format_cluster(cluster: Cluster, echo=click.echo):
    echo(f"{click.style(cluster.name, bold=True)} ({cluster.owner_profile.email})")
    echo(f"    ID:                    {cluster.guid}")

    health_status = cluster.status.health_status or "HEALTH_STATUS_UNKNOWN"
    echo(
        f"    Status:                {click.style(health_status, fg=_HEALTH_STATUS_COLORS.get(health_status, 'red'))}"
    )

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
    clusters_api = get_clusters_api(session=sc.session)
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
    cluster = resolve_hybrid_environment(sc, environment_name_or_id)
    sc.print(data=cluster, auto_printer=_format_cluster)


def resolve_hybrid_environment(
    sc: SessionContext,
    name_or_id: Optional[str],
    *,
    allow_auto_select: bool = False,
    require: bool = True,
) -> Optional[Cluster]:
    name_or_id = (name_or_id or "").strip()

    if not name_or_id and not allow_auto_select:
        raise ValueError("Hybrid environment name or ID must not be empty")

    clusters_api = get_clusters_api(session=sc.session)

    all_clusters = clusters_api.list_clusters(owned_only=False, expand=["owner"])
    if not name_or_id and allow_auto_select:
        if not all_clusters.clusters:
            raise ClusterError(
                "No hybrid environments are available. Please consult the documentation "
                "at https://docs.gretel.ai/operate-and-manage-gretel/gretel-hybrid for "
                "instructions on setting up a hybrid environment."
            )
        if len(all_clusters.clusters) > 1:
            raise ClusterError(
                "Multiple hybrid environments are available, but none was specified. Please "
                "run 'gretel hybrid environments list' to see a list of all available hybrid "
                "environments, and specify the name or ID of the one you wish to use."
            )
        return all_clusters.clusters[0]

    # First see if there is a match by GUID. This always gets preference.
    matching_by_id = [c for c in all_clusters.clusters if c.guid == name_or_id]
    if matching_by_id:
        return matching_by_id[0]
    matching_by_name = [c for c in all_clusters.clusters if c.name == name_or_id]
    if not matching_by_name:
        raise ClusterError(f"No hybrid environment with name '{name_or_id}' exists")
    if len(matching_by_name) > 1:
        raise ClusterError(
            f"Hybrid environment name '{name_or_id}' is ambiguous, run 'gretel hybrid "
            "environments list' to see all available hybrid environments, then specify the unique ID of the desired cluster."
        )

    return matching_by_name[0]
