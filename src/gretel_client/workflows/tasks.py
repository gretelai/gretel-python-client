from typing import Optional

from inflection import underscore
from pydantic import BaseModel

from gretel_client.workflows.configs.builder import build_registry
from gretel_client.workflows.configs.registry import Registry
from gretel_client.workflows.configs.workflows import Step

TaskConfig = BaseModel


class TaskRegistry:
    @classmethod
    def create(cls) -> Registry:
        registry = build_registry(TaskConfig, Registry)
        return registry()


def task_to_step(task: TaskConfig, inputs: Optional[list[str]] = None) -> Step:
    task_name = underscore(task.__class__.__name__)
    return Step(
        name=task_name.replace("_", "-"),
        task=task_name,
        config=task.model_dump(exclude_unset=True),
        inputs=inputs,
    )
