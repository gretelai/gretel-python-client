import inspect

from typing import cast, Type, TypeVar

from gretel_client.workflows.configs.registry import Registry

RegistryT = TypeVar("RegistryT", bound=Registry)


def build_registry(
    task_base_class: Type[object],
    original_registry_class: Type[RegistryT] = Registry,
) -> Type[RegistryT]:
    """
    The `Registry` class only provides base Task configs. This might not be
    very useful if you are trying to build more advanced capabilities on top
    of those tasks. `build_registry` provides a factory for creating
    new Task config registries, where each Task config can be a subclass of
    `task_base_class`.

    Args:
        task_base_class: The base class to apply to all static Task
            config members.
        original_registry_class: The original registry class to compose the
            new registry from.
    """

    class TaskVariantRegistry(original_registry_class):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    for name, value in original_registry_class.__dict__.items():
        if not name.startswith("__") and inspect.isclass(value):
            derived_class = type(value.__name__, (value, task_base_class), {})
            setattr(TaskVariantRegistry, name, derived_class)

    return cast(Type[RegistryT], TaskVariantRegistry)
