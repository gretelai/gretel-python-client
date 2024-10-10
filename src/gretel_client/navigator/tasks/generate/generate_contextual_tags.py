from pathlib import Path
from typing import Optional, Union

from pydantic import BaseModel, Field
from typing_extensions import Self

from gretel_client.gretel.config_setup import smart_load_yaml
from gretel_client.navigator.tasks.base import Task, TaskResults


class ContextualTag(BaseModel):
    name: str
    description: Optional[str] = None
    seed_values: list[str] = Field(default=[])
    total_num_tags: Optional[int] = None

    @classmethod
    def from_dicts(cls, tags: list[dict]) -> list[Self]:
        return [cls(**tag) for tag in tags]


class GenerateContextualTagsConfig(BaseModel):
    tags: list[ContextualTag]
    task_context: str


class GenerateContextualTags(Task):

    def __init__(
        self,
        tags: Union[str, Path, list[dict], list[ContextualTag]],
        task_context: str,
        workflow_label: Optional[str] = None,
    ):
        super().__init__(
            config=GenerateContextualTagsConfig(
                tags=self._check_and_get_tags(tags),
                task_context=task_context,
                workflow_label=workflow_label,
            )
        )

    @staticmethod
    def _check_and_get_tags(
        tags: Union[str, Path, list[dict], list[ContextualTag]]
    ) -> list[ContextualTag]:
        if isinstance(tags, (str, Path)):
            tags = smart_load_yaml(tags).get("tags")

        if not isinstance(tags, list):
            raise ValueError("`tags` must be a list of dicts or ContextualTag objects")

        # Convert dicts to ContextualTag objects to ensure they are valid.
        if all(isinstance(tag, dict) for tag in tags):
            tags = ContextualTag.from_dicts(tags)

        if not all(isinstance(tag, ContextualTag) for tag in tags):
            raise ValueError("`tags` must be a list of dicts or ContextualTag objects")

        return tags

    @property
    def name(self) -> str:
        return "generate_contextual_tags"

    def run(self) -> TaskResults:
        return self._run()
