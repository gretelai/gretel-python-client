from functools import partial
from typing import Any, Optional

from gretel_client.config import RunnerMode
from gretel_client.rest import models
from gretel_client.rest.api.projects_api import ProjectInvite, ProjectsApi
from gretel_client.rest.exceptions import ApiException


class HybridProjectsApi(ProjectsApi):
    """
    Hybrid wrapper for the projects api.

    Objects of this class behave like the regular projects API,
    with the following exceptions:
    - if a deployment user is configured, this user will be given
      admin access to all newly created projects.
    - all created models and record handlers have an implicit "hybrid"
      runner mode. If the creation request explicitly specifies a runner
      mode, this must be "hybrid", otherwise the creation will fail.
    - creation of project artifacts is not possible.
    """

    _deployment_user: Optional[str]
    _default_cluster_guid: Optional[str]

    def __init__(
        self,
        api: ProjectsApi,
        deployment_user: Optional[str] = None,
        default_cluster_guid: Optional[str] = None,
    ):
        """
        Constructor.

        Args:
            api: the regular projects API object.
            deployment_user: the email address of a user that should be added as
                an admin to newly created projects. This should be the user whose
                API key is used for authenticating a Gretel Hybrid deployment.
        """
        super().__init__(api.api_client)
        self._deployment_user = deployment_user
        self._default_cluster_guid = default_cluster_guid

        # The API object we inherit from does not define methods for invoking API
        # endpoints, but instead sets them as attributes in the constructor.
        # That means we cannot just override them as methods in this class, but
        # instead have to overwrite the fields *after* invoking the superclass
        # constructor. Likewise, we cannot invoke the original operations via super(),
        # but instead pass the original operation as the first argument.
        self.create_project = partial(self._create_project, self.create_project)
        self.create_artifact = partial(self._create_artifact, self.create_artifact)
        self.create_model = partial(self._create_model, self.create_model)
        self.create_record_handler = partial(
            self._create_record_handler, self.create_record_handler
        )

    def _create_project(self, super_create_project, *args, **kwargs):
        project: models.Project = kwargs["project"]
        if (runner_mode := project.get("runner_mode")) and RunnerMode.parse(
            runner_mode
        ) != RunnerMode.HYBRID:
            raise ValueError(
                f"invalid project runner mode '{runner_mode}', only '{RunnerMode.HYBRID}' is allowed"
            )

        project.runner_mode = RunnerMode.HYBRID.value
        if not project.get("cluster_guid") and self._default_cluster_guid:
            project.cluster_guid = self._default_cluster_guid

        resp = super_create_project(*args, **kwargs)
        if self._deployment_user:
            project_id = resp["data"]["id"]
            print(
                f"Adding deployment user {self._deployment_user} to newly created project {project_id}"
            )
            try:
                self.create_invite(
                    project_id=project_id,
                    project_invite=ProjectInvite(email=self._deployment_user, level=3),
                )
            except ApiException as ex:
                # If we try to invite ourselves, the API returns an error, but that case
                # is actually fine and should not result in a user-visible exception.
                if "Cannot invite yourself" not in ex.body:
                    raise

        return resp

    def _create_artifact(self, super_create_artifact, *args, **kwargs):
        raise Exception("project artifact upload is disabled in Hybrid mode")

    def _create_model(self, super_create_model, *args, **kwargs):
        runner_mode = kwargs.pop("runner_mode", None)
        if runner_mode and RunnerMode.parse(runner_mode) != RunnerMode.HYBRID:
            raise ValueError(
                f"invalid model runner mode '{runner_mode}', only '{RunnerMode.HYBRID}' is allowed"
            )

        kwargs["runner_mode"] = RunnerMode.HYBRID.value
        return super_create_model(*args, **kwargs)

    def _create_record_handler(self, super_create_record_handler, *args, **kwargs):
        runner_mode = kwargs.pop("runner_mode", None)
        if runner_mode and RunnerMode.parse(runner_mode) != RunnerMode.HYBRID:
            raise ValueError(
                f"invalid record handler runner mode '{runner_mode}', only '{RunnerMode.HYBRID}' is allowed"
            )

        kwargs["runner_mode"] = RunnerMode.HYBRID.value
        return super_create_record_handler(*args, **kwargs)
