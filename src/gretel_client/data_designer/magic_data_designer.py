"""Defines magic interactions with DataDesigner"""

import functools
import inspect
import json
import logging
import random

from copy import deepcopy
from dataclasses import dataclass
from itertools import cycle
from typing import Any, Generator, Generic, Optional, Self, TypeAlias, TypeVar

import rich

from rich.console import Console, Group
from rich.panel import Panel
from rich.pretty import Pretty
from rich.prompt import Prompt
from rich.syntax import Syntax
from rich.table import Table

from gretel_client.data_designer.constants import RICH_CONSOLE_THEME
from gretel_client.data_designer.types import (
    AIDDColumnT,
    LLMGenColumn,
    MagicColumnT,
    ModelSuite,
    Person,
    SamplerColumn,
)
from gretel_client.data_designer.utils import camel_to_snake
from gretel_client.helpers import experimental, is_jupyter
from gretel_client.workflows.builder import Message, WorkflowInterruption
from gretel_client.workflows.configs.base import ConfigBase
from gretel_client.workflows.configs.registry import Registry
from gretel_client.workflows.configs.tasks import (
    DataColumnFromSamplingConfig,
    ExistingColumn,
    ExistingColumns,
    GenerateColumnConfigFromInstruction,
    GenerateColumnFromTemplateV2Config,
    GenerateSamplingColumnConfigFromInstruction,
    OutputType,
    SamplingSourceType,
)
from gretel_client.workflows.configs.workflows import Globals
from gretel_client.workflows.manager import WorkflowManager

logger = logging.getLogger(__name__)

ACTION_FUN_NAMES = ["Greteling", "Vibing", "Magicking", "Noodling"]

EXPERIMENTAL_WARNING = """\
Thanks for trying the 🪄 Magic `DataDesigner` interface! This interface is experimental \
and will likely change in the future. Please let us know your feedback and any issues \
@ https://github.com/gretelai/gretel-python-client/issues/new.
"""

SAMPLER_DESCRIPTION_MAPPING = {
    SamplingSourceType.BERNOULLI: "Value sampled from a Bernoilli distribution.",
    SamplingSourceType.BINOMIAL: "Value sampled from a Binomial distribution.",
    SamplingSourceType.CATEGORY: "Value sampled from a categorical distribution.",
    SamplingSourceType.DATETIME: "Value uniformly sampled from a range of DateTimes.",
    SamplingSourceType.GAUSSIAN: "Value sampled from a Gaussian distribution.",
    SamplingSourceType.PERSON: "Data structure of information sampled from a randomly generated synthetic person.",
    SamplingSourceType.POISSON: "Value sampled from a Poisson distribution.",
    SamplingSourceType.SCIPY: "Value sampled from a Scipy.stats distribution.",
    SamplingSourceType.SUBCATEGORY: "Value sampled from a categorical distribution.",
    SamplingSourceType.TIMEDELTA: "Value uniformly sampled from a range of TimeDelta DateTimes.",
    SamplingSourceType.UNIFORM: "Value sampled from a uniform distribution.",
    SamplingSourceType.UUID: "Random UUID-derived identifier.",
}

# Aliasing
AIDDColumnConfigGenerationTask: TypeAlias = (
    GenerateColumnConfigFromInstruction | GenerateSamplingColumnConfigFromInstruction
)
DataDesignerT = TypeVar(
    "DataDesignerT"
)  # Prevent circular imports on DataDesigner object

EditableColumnT: TypeAlias = LLMGenColumn | SamplerColumn
EditTaskConfigT: TypeAlias = (
    GenerateColumnFromTemplateV2Config | DataColumnFromSamplingConfig
)


# Error handling
class MagicError(Exception): ...


class RemoteExecutionError(Exception): ...


def action_fun_name() -> str:
    """Have a good time 🍻"""
    return random.choice(ACTION_FUN_NAMES)


def pprint_column_name(name: str):
    return f"[italic deep_sky_blue1]{name}[/italic deep_sky_blue1]"


def pprint_user_instruction(column: str, instruction: str):
    console = Console()
    console.print(
        Panel(
            instruction,
            title=f"💬 {pprint_column_name(column)}",
            style="deep_sky_blue1",
            title_align="left",
        )
    )


def with_processing_animation(func):
    def wrapper(task, *args, **kwargs):
        console = Console()
        column_name = pprint_column_name(getattr(task, "name", ""))
        action_str = action_fun_name()
        status_string = f"{action_str} {column_name}"

        with console.status(status_string, spinner="toggle10") as status:
            # Execute the wrapped function
            result_or_gen = func(task, *args, **kwargs)

            # Check if the result is a generator
            if inspect.isgenerator(result_or_gen):
                final_result = None
                try:
                    while True:
                        # Get the next item from the generator
                        yielded_item = next(result_or_gen)

                        # If it's a string, treat it as a status update
                        if isinstance(yielded_item, str):
                            status_update_message = yielded_item
                            updated_status = f"{status_string} [cyan]({status_update_message})[/cyan]"
                            status.update(updated_status)
                        else:
                            pass  # Ignore non-string yields for status updates

                except StopIteration as e:
                    # Generator finished, capture the return value
                    final_result = e.value
                return final_result
            else:
                # If it's not a generator, return the result directly
                return result_or_gen

    functools.update_wrapper(wrapper, func)
    return wrapper


def cast_datacolumn_to_existingcolumn(data_column: AIDDColumnT) -> ExistingColumn:
    """Cast a AIDDColumnT to an ExistingColumn."""
    args = {
        "name": data_column.name,
        "description": data_column.name,
        "output_type": OutputType.TEXT,
        "output_format": None,
    }

    if isinstance(data_column, LLMGenColumn):
        # ... handle the data type conversion etc.
        description_format = (
            "LLM-Generated Text.\n"
            "Description: {short_description}\n"
            "Generation Prompt: {prompt}"
        )
        args["description"] = description_format.format(
            short_description=(
                data_column.description if data_column.description else data_column.name
            ),
            prompt=data_column.prompt,
        )

        args["output_type"] = data_column.output_type
        args["output_format"] = data_column.output_format

    elif isinstance(data_column, SamplerColumn):
        description = SAMPLER_DESCRIPTION_MAPPING.get(
            data_column.type, data_column.name
        )
        ## Also append some parameters
        description += " Sampler Parameters: {}".format(
            data_column.params.model_dump_json()
        )
        args["description"] = description

        if data_column.type == SamplingSourceType.PERSON:
            ## Replace the output config with the Person
            ## data structure.
            args["output_type"] = OutputType.STRUCTURED
            args["output_format"] = Person.model_json_schema()

    return ExistingColumn(**args)


def cast_column_cfg_to_edit_task_cfg(column_cfg: EditableColumnT) -> EditTaskConfigT:
    """Cast an existing data column into the server-side edit-task format.

    Args:

        task_cfg: An existing editable column configuration.

    Returns:
        EditTaskConfigT: A matching type that the server expects for Edit tasks.

    Raises:
        NotImplementedError: If the colum type is not editable.
    """
    if isinstance(column_cfg, LLMGenColumn):
        return GenerateColumnFromTemplateV2Config.model_validate(
            column_cfg.model_dump()
        )
    elif isinstance(column_cfg, SamplerColumn):
        col_cfg_dict = column_cfg.model_dump()
        col_cfg_dict["sampling_type"] = col_cfg_dict.pop("type")
        return DataColumnFromSamplingConfig.model_validate(col_cfg_dict)
    else:
        raise NotImplementedError(
            f"Task configuration editing not supported for columns of type {type(column_cfg)}"
        )


def pprint_datacolumn(column: AIDDColumnT, use_html: bool = True) -> None:
    """Pretty print the generation config.

    Assumes that a _rich_ implementation has already been made for the
    data column class.
    """
    if is_jupyter() and use_html:
        from IPython.display import display, HTML

        html_repr = column._repr_html_()
        if html_repr is not None:
            display(HTML(html_repr))
        pass
    else:
        console = Console(theme=RICH_CONSOLE_THEME)
        render_group = Group(column)
        console.print(render_group)


def make_panel_grid(items: list[Any], columns: int, syntax: Optional[str]) -> Table:
    """
    Creates a grid of panels

    Args:
        items (list): List of items to display.
        columns (int): Number of columns in the grid.

    Returns:
        Panel: A rich Panel containing the grid of inner Panels.
    """
    # Create a grid table with the specified number of columns.
    grid = Table.grid(expand=True)
    for _ in range(columns):
        grid.add_column()

    # Convert each item to its own Panel.
    # Try to json loads an item
    def _load_if_json(x):
        try:
            return json.loads(x)
        except (json.JSONDecodeError, TypeError):
            return x

    def render_output(x):
        if syntax is None:
            x = _load_if_json(x)
            if isinstance(x, str):
                return x
            else:
                return Pretty(x)
        else:
            return Syntax(x, syntax, theme="native", word_wrap=True)

    panels = [Panel(render_output(item), expand=True) for item in items]

    # Add rows to the grid: slice the panels list into rows.
    for i in range(0, len(panels), columns):
        row_panels = panels[i : i + columns]
        grid.add_row(*row_panels)

    # Wrap the entire grid in an outer Panel.
    return grid


def pprint_outputs(
    preview_samples, name: str, n: int = 6, syntax: Optional[str] = None
) -> None:
    """Pretty print generation outputs"""
    last_step_samples = preview_samples.dataset.df[name].values.tolist()
    console = Console(theme=RICH_CONSOLE_THEME)
    console.print(
        Panel(
            make_panel_grid(last_step_samples[:n], 3, syntax),
            title=f"🎲 Samples of {pprint_column_name(name)}",
            style="gold3",
        )
    )


def is_cant_find_tool_error(interruption: WorkflowInterruption) -> bool:
    """Check a workflow interruption for sampler failure."""
    return "could not find an appropriate sampler" in interruption.message.lower()


def is_model_usage_log_line(message: Message) -> bool:
    """Check if the message is a model usage log line"""
    return message.type == "log_line" and "Model usage" in message.payload["msg"]


@with_processing_animation
def remote_streaming_execute_stateless_task(
    task: ConfigBase,
    workflow_manager: WorkflowManager,
    output_type_name: str,
    globals: Globals = Globals(),
) -> Generator[str, None, dict]:
    """Execute a stateless task in streaming mode.

    Args:
        task (ConfigBase): An instanstiated task configuration from the task registry.
        workflow_manager: An instantiated workflow manager object from
            somewhere else.
        output_type_name (str): Specifies the output type that
            should be retrieved from the message stream.
        globals (optional, Globals): Some globals defintion to give to the
            workflwo builder.

    Returns:
        Whatever the output of this task run is.
    """
    processing_emojis = ["🛠️", "🧱", "🏗️", "🪄", "💭", "🧠", "🧐"]
    processing_emoji_cycle = cycle(processing_emojis)
    workflow = workflow_manager.builder(globals=globals)
    workflow.add_step(task)

    ## Now we need to call against the task
    for message in workflow.iter_preview():
        if isinstance(message, WorkflowInterruption):
            if is_cant_find_tool_error(message):
                raise MagicError(
                    "Could not not find an appropriate sampling column type for your request; try rephrasing."
                )
            else:
                raise RemoteExecutionError(message.message)

        if message.type == "log_line" and not is_model_usage_log_line(message):
            yield message.payload.get("msg", next(processing_emoji_cycle))
        elif message.type == output_type_name:
            return message.payload
        else:
            yield next(processing_emoji_cycle)

    # If we never encountered the specified output, then we have a problem
    raise RemoteExecutionError(
        "Stream halted before receiving output. Contact support if this issue persists."
    )


@dataclass
class DataDesignerState:
    """State management for the DD object.

    This object is used to save, edit, or restore the state of a DataDesigner
    object.

    Attributes:
        columns (dict[str, MagicColumnT]): A dictionary mapping of column
            names to column configuration definitions; includes any columns
            that may have been given in a seed dataset.

    Methods:
        from_object(dd_obj: DataDesigner): Class initialization method
            used to create a DataDesignerState object from an existing
            DataDesigner object.

        set_data_designer_object(dd_obj: DataDesigner): Update the
            state of the DataDesigner object in-place using this state
            object.

        fork(): Create a new copy of this DataDesignerState object.
    """

    columns: dict[str, MagicColumnT]

    @classmethod
    def from_object(cls, dd_obj) -> Self:
        """Create DataDesignerState from an existing DataDesigner object.

        Args:
            dd_obj (DataDesigner): An initialized DataDesigner class object.

        Returns:
            DataDesignerState: The current "state" of the DD object.
        """
        ## We need to copy out the raw dictionary
        local_copy = deepcopy(dd_obj._columns.data)
        return cls(columns={**local_copy})

    def set_data_designer_object(self, dd_obj) -> None:
        """Set the state of a DataDesigner object to exactly match this state.

        Args:
            dd_obj (DataDesigner): An initialized DataDesigner class object.
        """
        ## Don't trigger the _columns update callback
        dd_obj._columns.data = deepcopy(self.columns)

    def fork(self):
        """Forks a new state copy."""
        return DataDesignerState(columns=deepcopy(self.columns))


class MagicDataDesignerEditor(Generic[DataDesignerT]):
    dataset_objective: str
    row_entity: str
    model_suite: ModelSuite
    _dd_obj: DataDesignerT
    _source_dd_state: DataDesignerState
    _working_dd_state: DataDesignerState

    def __init__(
        self,
        dd_obj: DataDesignerT,
        *,
        dataset_objective: str = "",
        row_entity: str = "",
        model_suite: ModelSuite = ModelSuite.APACHE_2_0,
    ):
        self.dataset_objective = dataset_objective
        self.row_entity = row_entity
        self.model_suite = model_suite
        self._task_registry = Registry()
        self._dd_obj = dd_obj

    def reset(self):
        """Start over from scratch."""
        self._source_dd_state = DataDesignerState.from_object(self._dd_obj)
        self._working_dd_state = self._source_dd_state.fork()

    def save(self):
        """Save current state into the source state."""
        self._working_dd_state.set_data_designer_object(self._dd_obj)

        ## Print information on newly added columns.
        new_columns = set(self._working_dd_state.columns.keys()) - set(
            self._source_dd_state.columns.keys()
        )
        for new_column in new_columns:
            rich.print(
                f"[bold green]🪄 Column [italic deep_sky_blue1]{new_column}[/italic deep_sky_blue1] updated in source DataDesigner![/bold green]"
            )

        self.reset()

    def _run_and_display_preview(self, name: str, syntax: Optional[str] = None) -> None:
        # Export and then reset. Wonder if this could be a context manager.
        self._working_dd_state.set_data_designer_object(self._dd_obj)

        console = Console(theme=RICH_CONSOLE_THEME)
        status_string = f"{action_fun_name()} samples of {pprint_column_name(name)}..."

        with console.status(status_string, spinner="toggle10"):
            outs = self._dd_obj.preview()

        self._source_dd_state.set_data_designer_object(self._dd_obj)
        pprint_outputs(outs, name, n=3, syntax=syntax)

    def _instruction_to_data_column(
        self,
        task: AIDDColumnConfigGenerationTask,
        edit_instruction: Optional[str] = None,
        previous_attempt: Optional[EditableColumnT] = None,
    ) -> AIDDColumnT:
        """Single call to get a generation config."""
        _task = deepcopy(task)

        if isinstance(task, GenerateColumnConfigFromInstruction):
            aidd_column_type = LLMGenColumn
            output_type_name = camel_to_snake(
                GenerateColumnFromTemplateV2Config.__name__
            )
            if edit_instruction and previous_attempt:
                _task.instruction = edit_instruction
                _task.edit_task = cast_column_cfg_to_edit_task_cfg(previous_attempt)
        elif isinstance(task, GenerateSamplingColumnConfigFromInstruction):
            aidd_column_type = SamplerColumn
            output_type_name = "data_column_from_sampling_config"
            if edit_instruction and previous_attempt:
                assert isinstance(previous_attempt, SamplerColumn)
                _task.instruction = edit_instruction
                _task.edit_task = cast_column_cfg_to_edit_task_cfg(previous_attempt)
        else:
            raise NotImplementedError(
                f"Magic is not yet implemented for task type: {type(task)}"
            )

        task_output = remote_streaming_execute_stateless_task(
            task=_task,
            workflow_manager=self._dd_obj._workflow_manager,
            output_type_name=output_type_name,
            globals=Globals(
                model_suite=self.model_suite,
            ),
        )

        if isinstance(task, GenerateSamplingColumnConfigFromInstruction):
            task_output["type"] = task_output.pop("sampling_type")

        new_data_column = aidd_column_type.model_validate(task_output)

        ## Double check the data column to make sure thats its model_suite
        ## matches what was specified in the original dd object.
        if hasattr(new_data_column, "model_suite"):
            new_data_column.model_suite = self._dd_obj.model_suite

        pprint_datacolumn(new_data_column)
        self._working_dd_state.columns[task.name] = new_data_column

        return new_data_column

    def _instruction_loop_for_aidd_column(
        self,
        task,
        *,
        interactive: bool,
        preview: bool,
    ) -> Optional[AIDDColumnT]:
        """Common call loop for executing the instruction call task."""
        commands = [
            "accept",
            "cancel",
            "start-over",
            "retry",
            "preview",
            "preview-on",
            "preview-off",
        ]
        command = "retry"
        terminal_commands = ["accept", "cancel"]
        fail_commands = ["cancel"]
        commands_str = "/".join(commands)

        if isinstance(task, GenerateColumnConfigFromInstruction):
            original_instruction = task.instruction
        elif isinstance(task, GenerateSamplingColumnConfigFromInstruction):
            original_instruction = task.instruction
        else:
            raise NotImplementedError(
                f"Magic is not yet implemented for task type: {type(task)}"
            )

        new_data_column = None
        last_edit_instruction = None
        while command not in terminal_commands:
            if command not in commands:
                last_edit_instruction = command
                pprint_user_instruction(task.name, last_edit_instruction)
                new_data_column = self._instruction_to_data_column(
                    task,
                    edit_instruction=command,
                    previous_attempt=new_data_column,
                )
            elif command == "preview":
                output_type = getattr(new_data_column, "output_type", None)
                output_format = getattr(new_data_column, "output_format", None)
                syntax = output_format if output_type == "code" else None
                self._run_and_display_preview(task.name, syntax=syntax)
            elif command == "preview-on":
                preview = True
                rich.print(
                    "[bold]Sample previews toggled [italic green]on[/italic green].[/bold]"
                )
            elif command == "preview-off":
                preview = False
                rich.print(
                    "[bold]Sample previews toggled [italic red]off[/italic red].[/bold]"
                )
            elif command == "retry":
                pprint_user_instruction(
                    task.name,
                    (
                        last_edit_instruction
                        if last_edit_instruction
                        else original_instruction
                    ),
                )
                new_data_column = self._instruction_to_data_column(
                    task,
                    edit_instruction=last_edit_instruction,
                    previous_attempt=new_data_column,
                )
            elif command == "start-over":
                last_edit_instruction = None
                rich.print("[bold]⏮️ Starting over from the top.[/bold]")
                pprint_user_instruction(task.name, original_instruction)
                new_data_column = self._instruction_to_data_column(task)

            ## Handle preview
            if preview:
                output_type = getattr(new_data_column, "output_type", None)
                output_format = getattr(new_data_column, "output_format", None)
                syntax = output_format if output_type == "code" else None
                self._run_and_display_preview(task.name, syntax=syntax)

            if not interactive:
                ## Halt without prompting, mission accomplished.
                break

            command = Prompt.ask(
                f"[bold]\[{commands_str}] or your instructions[/bold]", default="retry"
            )

        if command in fail_commands:
            return None

        return new_data_column

    @experimental(message=EXPERIMENTAL_WARNING, emoji="🧪")
    def add_column(
        self,
        name: str,
        instruction: str,
        *,
        must_depend_on: Optional[list[str]] = None,
        preview: bool = False,
        save: bool = True,
        interactive: bool = False,
    ) -> DataDesignerT:
        if self.is_known_column_name(name) and not self.is_editable_column(name):
            _col = self._dd_obj.get_column(name)
            _type_name = _col.__class__.__name__
            raise KeyError(
                f"The column '{name}' already exists as type {_type_name}, it cannot be updated with this function. "
            )

        if must_depend_on is None:
            must_depend_on = []

        for dependency_column_name in must_depend_on:
            if not self.is_known_column_name(dependency_column_name):
                raise KeyError(
                    f"Required dependency {dependency_column_name} does not exist."
                )

        ## Get previously existing column if present
        edit_task = None
        if self.is_known_column_name(name):
            edit_data_column = self._dd_obj.get_column(name)
            edit_task = GenerateColumnFromTemplateV2Config.model_validate(
                edit_data_column.model_dump()
            )

        task = self._task_registry.GenerateColumnConfigFromInstruction(
            name=name,
            instruction=instruction,
            must_depend_on=must_depend_on,
            existing_columns=self.existing_columns,
            edit_task=edit_task,
        )

        new_data_column = self._instruction_loop_for_aidd_column(
            task, interactive=interactive, preview=preview
        )

        if new_data_column is None:
            self._working_dd_state = self._source_dd_state.fork()
            rich.print(f"🗑️ Trashed {pprint_column_name(name)}")
        elif not save:
            rich.print(
                f"🪄 Column {pprint_column_name(name)} buffered! Use .save() to apply later."
            )
        else:
            self.save()

        return self._dd_obj

    @experimental(message=EXPERIMENTAL_WARNING, emoji="🧪")
    def add_sampling_column(
        self,
        name: str,
        instruction: str,
        *,
        preview: bool = False,
        save: bool = True,
        interactive: bool = False,
    ) -> DataDesignerT:
        if self.is_known_column_name(name):
            _col = self._dd_obj.get_column(name)

            if not isinstance(_col, SamplerColumn):
                _type_name = _col.__class__.__name__
                raise KeyError(
                    f"The column '{name}' already exists as type {_type_name}, it cannot be updated with this function. "
                )

        ## Get previously existing column if present. If it is,
        ## then we know at this point that it is a SamplerColumn
        edit_task = None
        if self.is_known_column_name(name):
            edit_data_column = self._dd_obj.get_column(name)
            edit_task = DataColumnFromSamplingConfig(
                name=edit_data_column.name,
                sampling_type=edit_data_column.type,
                params=edit_data_column.params.model_dump(),
            )

        task = self._task_registry.GenerateSamplingColumnConfigFromInstruction(
            name=name,
            instruction=instruction,
            edit_task=edit_task,
        )

        new_data_column = self._instruction_loop_for_aidd_column(
            task, interactive=interactive, preview=preview
        )

        if new_data_column is None:
            self._working_dd_state = self._source_dd_state.fork()
            rich.print(f"🗑️ Trashed {pprint_column_name(name)}")
        elif not save:
            rich.print(
                f"🪄 Column {pprint_column_name(name)} buffered! Use .save() to apply later."
            )
        else:
            self.save()

        return self._dd_obj

    @property
    def editable_columns(self) -> list[AIDDColumnT]:
        """List out columns that can be edited with Magick.

        Presently, only LLM prompting columns can be edited.
        """

        return self._dd_obj._llm_gen_columns

    @property
    def known_columns(self) -> list[AIDDColumnT]:
        """List all known columns in the current DD state."""
        return [
            *self._dd_obj._seed_columns,
            *self._dd_obj._sampler_columns,
            *self._dd_obj._llm_gen_columns,
            *self._dd_obj._llm_judge_columns,
            *self._dd_obj._code_validation_columns,
        ]

    def is_known_column_name(self, name: str) -> bool:
        return name in [column.name for column in self.known_columns]

    def is_editable_column(self, name: str) -> bool:
        return name in [column.name for column in self.editable_columns]

    @property
    def existing_columns(self) -> ExistingColumns:
        """Cast known columns to ExistingColumns"""
        return ExistingColumns(
            variables=[
                cast_datacolumn_to_existingcolumn(column)
                for column in self.known_columns
            ]
        )
