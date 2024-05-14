from typing import Optional, Type, TypeVar

from gretel_client._hybrid.asymmetric import AsymmetricCredentialsEncryption
from gretel_client._hybrid.connections_api import HybridConnectionsApi
from gretel_client._hybrid.creds_encryption import BaseCredentialsEncryption
from gretel_client._hybrid.projects_api import HybridProjectsApi
from gretel_client._hybrid.workflows_api import HybridWorkflowsApi
from gretel_client.config import (
    ClientConfig,
    configure_session,
    DelegatingClientConfig,
    get_session_config,
    RunnerMode,
    set_session_config,
)
from gretel_client.rest.api.projects_api import ProjectsApi
from gretel_client.rest_v1.api.connections_api import ConnectionsApi
from gretel_client.rest_v1.api.projects_api import ProjectsApi as ProjectsV1Api
from gretel_client.rest_v1.api.workflows_api import WorkflowsApi

T = TypeVar("T", bound=Type)


def hybrid_session_config(
    creds_encryption: Optional[BaseCredentialsEncryption] = None,
    deployment_user: Optional[str] = None,
    default_cluster_guid: Optional[str] = None,
    session: Optional[ClientConfig] = None,
) -> ClientConfig:
    """
    Configures a Gretel client session for hybrid mode.

    For ensuring that all operations run in hybrid mode, it is strongly
    recommended to call ``set_session_config`` with the return value
    of this function afterwards.

    Args:
        creds_encryption: The credentials encryption mechanism to use.
            This is generally cloud provider-specific.
        deployment_user: the user used for the Gretel Hybrid deployment.
            Can be omitted if this is the same as the current user.
        default_cluster_guid: the cluster GUID to use for newly created
            projects by default.
        session:
            The regular Gretel client session. If this is omitted, the
            default session obtained via ``get_session_config()`` will be
            used.

    Returns:
        The hybrid-configured session.
    """

    if session is None:
        session = get_session_config()

    return _HybridSessionConfig(
        session, creds_encryption, deployment_user, default_cluster_guid
    )


def configure_hybrid_session(
    *args,
    creds_encryption: Optional[BaseCredentialsEncryption] = None,
    deployment_user: Optional[str] = None,
    default_cluster_guid: Optional[str] = None,
    **kwargs,
):
    """
    Sets up the main Gretel client session and configures it for hybrid use.

    This function can be used in place of ``configure_session``. It supports
    all arguments of the former, and in addition to that also the hybrid
    configuration parameters supported by ``hybrid_session_config``.

    After this function returns, the main session object used by Gretel SDK
    functions will be a session object configured for hybrid use.

    Args:
        creds_encryption: the credentials encryption mechanism to use for Hybrid
            connections.
        deployment_user: the deployment user to add to all newly created projects.
        default_cluster_guid: the GUID of the cluster to use by default for newly created projects.
        args: positional arguments to pass on to ``configure_session``.
        kwargs: keyword arguments to pass on to ``configure_session``.
    """
    default_runner = RunnerMode.parse(kwargs.pop("default_runner", RunnerMode.HYBRID))
    if default_runner != RunnerMode.HYBRID:
        raise ValueError(
            f"default runner mode {default_runner} isn't allowed in hybrid mode, change to '{RunnerMode.HYBRID}' or omit"
        )
    artifact_endpoint = kwargs.pop("artifact_endpoint", "none")
    if artifact_endpoint == "cloud":
        raise ValueError(
            "'cloud' artifact endpoint isn't allowed in hybrid mode, change to an object store location, or to 'none' to disable artifact uploads"
        )
    configure_session(
        *args,
        default_runner=default_runner,
        artifact_endpoint=artifact_endpoint,
        **kwargs,
    )
    set_session_config(
        hybrid_session_config(
            creds_encryption=creds_encryption,
            deployment_user=deployment_user,
            default_cluster_guid=default_cluster_guid,
        )
    )


class _HybridSessionConfig(DelegatingClientConfig):
    """
    Client configuration with hybrid settings.

    This class can be used as a drop-in replacement of ``ClientConfig`` for all means
    and purposes.
    """

    _creds_encryption: BaseCredentialsEncryption
    _deployment_user: Optional[str]
    _default_cluster_guid: Optional[str]

    def __init__(
        self,
        session: ClientConfig,
        creds_encryption: Optional[BaseCredentialsEncryption] = None,
        deployment_user: Optional[str] = None,
        default_cluster_guid: Optional[str] = None,
    ):
        super().__init__(session)
        self._creds_encryption = (
            creds_encryption
            if creds_encryption is not None
            else AsymmetricCredentialsEncryption(
                projects_api=session.get_v1_api(ProjectsV1Api)
            )
        )
        self._deployment_user = deployment_user
        self._default_cluster_guid = default_cluster_guid

    def get_api(self, api_interface: Type[T], *args, **kwargs) -> T:
        api = super().get_api(api_interface, *args, **kwargs)
        if api_interface == ProjectsApi:
            return HybridProjectsApi(
                api, self._deployment_user, self._default_cluster_guid
            )
        return api

    def get_v1_api(self, api_interface: Type[T]) -> T:
        api = super().get_v1_api(api_interface)
        if api_interface == WorkflowsApi:
            return HybridWorkflowsApi(api)
        if api_interface == ConnectionsApi:
            return HybridConnectionsApi(api, self._creds_encryption)
        return api
