"""
High level API for interacting with a Gretel Project
"""
import uuid

from contextlib import contextmanager
from functools import wraps
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Type, TypeVar, Union

from backports.cached_property import cached_property

from gretel_client.cli.utils.parser_utils import (
    DataSourceTypes,
    ref_data_factory,
    RefDataTypes,
)
from gretel_client.config import get_logger, get_session_config, RunnerMode
from gretel_client.dataframe import _DataFrameT
from gretel_client.projects.artifact_handlers import (
    ArtifactsHandler,
    cloud_handler,
    CloudArtifactsHandler,
    hybrid_handler,
    HybridArtifactsHandler,
)
from gretel_client.projects.common import f, validate_data_source
from gretel_client.projects.exceptions import GretelProjectError
from gretel_client.projects.models import Model
from gretel_client.rest import models
from gretel_client.rest.api.projects_api import ProjectsApi
from gretel_client.rest.exceptions import UnauthorizedException
from gretel_client.users.users import get_me

DATA = "data"
PROJECTS = "projects"
PROJECT = "project"
MODELS = "models"


logger = get_logger(__name__)


def check_not_deleted(func):
    @wraps(func)
    def wrap(self, *args, **kwargs):
        if self._deleted:
            raise GretelProjectError(
                "Cannot call method. The project has been marked for deletion."
            )
        return func(self, *args, **kwargs)

    return wrap


MT = TypeVar("MT", dict, Model)


class Project:
    """Represents Gretel project. In general you should not have
    to init this class directly, but can make use of the factory
    method from ``get_project``.

    Args:
        name: The unique name of the project. This is either set by you or auto
            managed by Gretel
        project_id: The unique project id of your project. This is managed by
            gretel and never changes.
        desc: A short description of the project
        display_name: The main display name used in the Gretel Console for your project
    """

    def __init__(
        self,
        *,
        name: str,
        project_id: str,
        project_guid: Optional[str] = None,
        desc: Optional[str] = None,
        display_name: Optional[str] = None,
    ):
        self.client_config = get_session_config()
        self.projects_api = self.client_config.get_api(ProjectsApi)
        self.name = name
        self.project_id = project_id
        self.project_guid = project_guid
        self.description = desc
        self.display_name = display_name
        self._deleted = False

    @cached_property
    def default_artifacts_handler(self) -> ArtifactsHandler:
        default_runner = self.client_config.default_runner

        if default_runner == RunnerMode.HYBRID:
            return self.hybrid_artifacts_handler
        elif default_runner == RunnerMode.CLOUD:
            return self.cloud_artifacts_handler
        else:
            raise GretelProjectError(
                f"Artifact handling is not supported under {default_runner} runner mode. "
                "Please update the default runner in your configuration to `cloud` or `hybrid` to work with artifacts."
            )

    @cached_property
    def cloud_artifacts_handler(self) -> CloudArtifactsHandler:
        return cloud_handler(self)

    @cached_property
    def hybrid_artifacts_handler(self) -> HybridArtifactsHandler:
        return hybrid_handler(self)

    @check_not_deleted
    def delete(self, *args, **kwargs):
        """Deletes a project. After the project has been deleted, functions
        relying on a project will fail with a ``GretelProjectError`` exception.

        Note: Deleting projects is asynchronous. It may take a few seconds
        for the project to be deleted by Gretel services.
        """
        # todo(dn): remove in 0.9 release
        if kwargs.get("include_models") or len(args) > 0:
            logger.warning(
                "``include_models`` is deprecated and will be removed in the 0.9 release."
            )
        self.projects_api.delete_project(project_id=self.project_id)
        self._deleted = True

    @check_not_deleted
    def get_console_url(self) -> str:
        """Returns web link to access the project from Gretel's console."""
        console_base = self.client_config.endpoint.replace("api", "console").replace(
            "cloud", "ai"
        )
        return f"{console_base}/{self.project_guid}"

    @property
    def as_dict(self) -> dict:
        """Returns a dictionary representation of the project."""
        return {
            "name": self.name,
            "project_id": self.project_id,
            "display_name": self.display_name,
            "desc": self.description,
            "console_url": self.get_console_url(),
        }

    @check_not_deleted
    def info(self) -> dict:
        """Return details about the project."""
        return self.projects_api.get_project(project_id=self.name).get(DATA)

    @check_not_deleted
    def search_models(
        self,
        factory: Type[MT] = Model,
        limit: int = 100,
        model_name: str = "",
    ) -> Iterator[MT]:
        """Search for project models.

        Args:
            factory: Determines what type of Model representation is returned.
                If ``Model`` is passed, a ``Model`` will be returned. If ``dict``
                is passed, a dictionary representation of the search results
                will be returned.
            limit: Limits the number of project models to return
            model_name: Name of the model to try and match on (partial match)
        """
        if factory not in (dict, Model):
            raise ValueError("factory must be one of ``dict`` or ``Model``.")

        api_args = {"project_id": self.name, "limit": limit}
        if model_name:
            api_args["model_name"] = model_name

        result = self.projects_api.get_models(**api_args)
        searched_models = result.get(DATA).get(MODELS)

        for model in searched_models:
            if factory == Model:
                model = self.get_model(model_id=model[f.UID])
            yield model

    @check_not_deleted
    def get_model(self, model_id: str) -> Model:
        """Lookup and return a Project ``Model`` by it's ``model_id``.

        Args:
            model_id: The ``model_id`` to lookup
        """
        return Model(project=self, model_id=model_id)

    @check_not_deleted
    def create_model_obj(
        self,
        model_config: Union[str, Path, dict],
        data_source: Optional[DataSourceTypes] = None,
        ref_data: Optional[RefDataTypes] = None,
    ) -> Model:
        """Creates a new model object. This will not submit the model
        to Gretel's cloud API. Please refer to the ``Model`` docs for
        more information detailing how to submit the model.

        Args:
            model_config: Specifies a model config. For more information
                about model configs, please refer to our doc site,
                https://docs.gretel.ai/reference/model-configurations.
            data_source: Defines the model data_source. If the model_config
                already has a data_source defined, this property will
                override the existing one.
            ref_data: An Optional str, dict, dataframe or list of reference data sources
                to upload for the job.
        """
        _model = Model(model_config=model_config, project=self)
        if (
            not isinstance(data_source, _DataFrameT)
            and data_source
            and not isinstance(data_source, str)
        ):
            raise ValueError("data_source must be a str or dataframe")
        if data_source is not None:
            _model.data_source = data_source
        ref_data_obj = ref_data_factory(ref_data)
        _model.ref_data = ref_data_obj
        return _model

    @property
    @check_not_deleted
    def artifacts(self) -> List[dict]:
        """Returns a list of project artifacts."""
        return self.default_artifacts_handler.list_project_artifacts()

    def upload_artifact(
        self,
        artifact_path: Union[Path, str, _DataFrameT],
        _validate: bool = True,
        _artifacts_handler: Optional[ArtifactsHandler] = None,
    ) -> str:
        """Resolves and uploads the data source specified in the
        model config.

        Returns:
            A Gretel artifact key.
        """
        if _validate and not isinstance(artifact_path, _DataFrameT):
            validate_data_source(artifact_path)
        artifacts_handler = _artifacts_handler or self.default_artifacts_handler
        return artifacts_handler.upload_project_artifact(artifact_path)

    def delete_artifact(self, key: str):
        """Deletes a project artifact.

        Args:
            key: Artifact key to delete.
        """
        return self.default_artifacts_handler.delete_project_artifact(key)

    def get_artifact_link(self, key: str) -> str:
        """Returns a link to download a project artifact.

        Args:
            key: Project artifact key to generate download url for.

        Returns:
            A signed URL that may be used to download the given
            project artifact.
        """
        return self.default_artifacts_handler.get_project_artifact_link(key)

    def get_artifact_manifest(
        self, key: str, retry_on_not_found: bool = True, retry_on_pending: bool = True
    ) -> dict:
        return self.default_artifacts_handler.get_project_artifact_manifest(
            key, retry_on_not_found, retry_on_pending
        )


def search_projects(limit: int = 200, query: Optional[str] = None) -> List[Project]:
    """Searches for project.

    Args:
        limit: The max number of projects to return.
        query: String filter applied to project names.
        client_config: Can be used to override local Gretel config.

    Returns:
        A list of projects.
    """
    api = get_session_config().get_api(ProjectsApi)
    params: Dict[str, Any] = {"limit": limit}
    if query:
        params["query"] = query
    projects = api.search_projects(**params)
    return [
        Project(
            name=p.get("name"),
            project_id=p.get("_id"),
            project_guid=p.get("guid"),
            desc=p.get("description"),
            display_name=p.get("display_name"),
        )
        for p in projects.get(DATA).get(PROJECTS)
    ]


def create_project(
    *,
    name: Optional[str] = None,
    desc: Optional[str] = None,
    display_name: Optional[str] = None,
) -> Project:
    """
    Excplit project creation. This function will simply call
    the API endpoint and will raise any HTTP exceptions upstream.
    """
    api = get_session_config().get_api(ProjectsApi)

    payload = {}
    if name:
        payload["name"] = name
    if desc:
        payload["description"] = desc
    if display_name:
        payload["display_name"] = display_name

    resp = api.create_project(project=models.Project(**payload))
    project = api.get_project(project_id=resp.get(DATA).get("id"))

    proj = project.get(DATA).get(PROJECT)

    return Project(
        name=proj.get("name"),
        project_id=proj.get("_id"),
        project_guid=proj.get("guid"),
        desc=proj.get("description"),
        display_name=proj.get("display_name"),
    )


def get_project(
    *,
    name: Optional[str] = None,
    create: bool = False,
    desc: Optional[str] = None,
    display_name: Optional[str] = None,
) -> Project:
    """Used to get or create a Gretel project.

    Args:
        name: The unique name of the project. This is either set by you or auto
            managed by Gretel.
        create: If create is set to True the function will create the project if
            it doesn't exist.
        project_id: The unique project id of your project. This is managed by
            gretel and never changes.
        desc: A short description of the project
        display_name: The main display name used in the Gretel Console for your project
    Returns:
        A project instance.
    """
    if not name and not create:
        raise ValueError("Must provide a name or create flag!")

    api = get_session_config().get_api(ProjectsApi)
    project = None

    project_args = {}
    if create:
        if desc:
            project_args["description"] = desc
        if display_name:
            project_args["display_name"] = display_name

    if not name and create:
        resp = api.create_project(project=models.Project(**project_args))
        project = api.get_project(project_id=resp.get(DATA).get("id"))

    if name:
        try:
            project = api.get_project(project_id=name)
        except UnauthorizedException:
            if create:
                project_args["name"] = name
                resp = api.create_project(project=models.Project(**project_args))
                project = api.get_project(project_id=resp.get(DATA).get("id"))
            else:
                raise GretelProjectError(f"Could not get project using '{name}'.")

    if not project:
        raise GretelProjectError(f"Could not get or create project using '{name}'.")

    p = project.get(DATA).get(PROJECT)

    return Project(
        name=p.get("name"),
        project_id=p.get("_id"),
        project_guid=p.get("guid"),
        desc=p.get("description"),
        display_name=p.get("display_name"),
    )


@contextmanager
def tmp_project():
    """A temporary project context manager.  Create a new project
    that can be used inside of a "with" statement for temporary purposes.
    The project will be deleted from Gretel Cloud when the scope is exited.

    Example::

        with tmp_project() as proj:
            model = proj.create_model_obj()
    """
    project = get_project(create=True)
    try:
        yield project
    finally:
        project.delete()


def create_or_get_unique_project(
    *, name: str, desc: Optional[str] = None, display_name: Optional[str] = None
) -> Project:
    """
    Helper function that provides a consistent experience for creating
    and fetching a project with the same name. Given a name of a project,
    this helper will fetch the current user's ID and use that as a suffix
    in order to create a project name unique to that user. Once the project
    is created, if it can be fetched, it will not be re-created over and
    over again.

    Args:
        name: The name of the project, which will have the User's ID appended
            to it automatically.
        desc: Description of the project.
        display_name: If None, the display name will be set equal to the value
            of ``name`` _before_ the user ID is appended.

    NOTE:
        The ``desc`` and ``display_name`` parameters will only be used when
        the project is first created. If the project already exists, these
        params will have no affect.
    """
    current_user_dict = get_me()
    unique_suffix = current_user_dict["_id"][9:]
    target_name = f"{name}-{unique_suffix}"

    try:
        project = get_project(name=target_name)
        return project
    except GretelProjectError:
        # Project name not found
        pass
    except Exception:
        raise

    # Try and create the project if we coud not find it
    # originally
    project = create_project(
        name=target_name, display_name=display_name or name, desc=desc
    )
    return project
