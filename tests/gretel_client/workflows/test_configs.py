from gretel_client.workflows.configs import tasks, workflows
from gretel_client.workflows.configs.builder import build_registry
from gretel_client.workflows.configs.registry import Registry


def test_can_load_workflow_configs():
    assert workflows is not None


def test_can_load_task_configs():
    assert tasks is not None


def test_concrete_tasks():
    assert Registry is not None


def test_can_instantiate_config():
    workflow = workflows.Workflow(
        name="test",
        steps=[
            workflows.Step(
                name="test-step",
                task="dummy_task_with_inputs",
                config=Registry.IdGenerator().model_dump(),
            )
        ],
    )

    assert len(workflow.steps) == 1


def test_instantiate_registry_with_factory():

    class Task:
        def hello_world(self) -> str:
            return "hello world"

    my_registry = build_registry(Task, Registry)
    id_generator = my_registry.IdGenerator(num_records=10)

    assert isinstance(id_generator, Task)
    assert id_generator.hello_world() == "hello world"
    assert id_generator.num_records == 10
