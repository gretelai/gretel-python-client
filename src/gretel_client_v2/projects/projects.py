"""
High level API for interacting with a Gretel Project
"""
from typing import Any, Dict, List
from functools import wraps

from gretel_client_v2.rest.api.projects_api import ProjectsApi
from gretel_client_v2.rest.exceptions import UnauthorizedException, NotFoundException
from gretel_client_v2.rest import models
from gretel_client_v2.config import get_session_config


DATA = "data"
PROJECTS = "projects"
PROJECT = "project"


class GretelProjectError(Exception):
    ...


def check_not_deleted(func):
    @wraps(func)
    def wrap(self, *args, **kwargs):
        if self._deleted:
            raise GretelProjectError(
                "Cannot call method. The project has been marked for deletion."
            )
        return func(self, *args, **kwargs)

    return wrap


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
        client_config: An instance of a _ClientConfig. This can be used to override any
            default connection configuration.
    """

    def __init__(
        self, *, name: str, project_id: str, desc: str = None, display_name: str = None
    ):
        self.client_config = get_session_config()
        self.projects_api = self.client_config.get_api(ProjectsApi)
        self.name = name
        self.project_id = project_id
        self.description = desc
        self.display_name = display_name
        self._deleted = False

    @check_not_deleted
    def delete(self):
        self.projects_api.delete_project(project_id=self.project_id)
        self._deleted = True

    @check_not_deleted
    def get_console_url(self) -> str:
        """Returns web link to access the project from Gretel's console."""
        console_base = self.client_config.endpoint.replace("api", "console")
        return f"{console_base}/{self.name}"

    @property
    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "project_id": self.project_id,
            "display_name": self.display_name,
            "desc": self.description,
            "console_url": self.get_console_url()
        }

    def info(self) -> dict:
        return self.projects_api.get_project(project_id=self.name)["data"]


def search_projects(limit: int = 200, query: str = None) -> List[Project]:
    """Searches for project

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
            desc=p.get("description"),
            display_name=p.get("display_name"),
        )
        for p in projects.get(DATA).get(PROJECTS)
    ]


def get_project(
    *,
    name: str = None,
    create: bool = False,
    desc: str = None,
    display_name: str = None,
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
        client_config: An instance of a _ClientConfig. This can be used to override any
            default connection configuration.
    Returns:
        A project instance.
    """
    if not name and not create:
        raise ValueError("Must provide a name or create flag!")

    api = get_session_config().get_api(ProjectsApi)
    project = None

    if not name and create:
        resp = api.create_project()
        project = api.get_project(project_id=resp.get(DATA).get("id"))

    if name:
        try:
            project = api.get_project(project_id=name)
        except (UnauthorizedException, NotFoundException):
            if create:
                resp = api.create_project(
                    models.Project(
                        name=name, display_name=display_name, description=desc
                    )
                )
                project = api.get_project(project_id=resp.get(DATA).get("id"))
            else:
                raise

    if not project:
        raise GretelProjectError("Could not get or create project.")

    p = project.get(DATA).get(PROJECT)

    return Project(
        name=p.get("name"),
        project_id=p.get("_id"),
        desc=p.get("description"),
        display_name=p.get("display_name"),
    )
