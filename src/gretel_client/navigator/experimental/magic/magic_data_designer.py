"""Defines magic interactions with DataDesigner
"""

import json
import logging
import random

from copy import deepcopy
from typing import Any, Generic, Optional, TypeVar, Union

import rich

from rich.console import Console, Group
from rich.panel import Panel
from rich.pretty import Pretty
from rich.prompt import Prompt
from rich.syntax import Syntax
from rich.table import Table

from gretel_client.navigator.data_designer.data_column import GeneratedDataColumn
from gretel_client.navigator.experimental import experimental
from gretel_client.navigator.log import get_logger
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.generate.generate_column_from_template import (
    GenerateColumnFromTemplateConfig,
)
from gretel_client.navigator.tasks.generate_column_config_from_instruction import (
    GenerateColumnConfigFromInstruction,
)
from gretel_client.navigator.tasks.types import (
    CategoricalDataSeeds,
    DataConfig,
    ExistingColumn,
    ExistingColumns,
    ModelSuite,
    OutputColumnType,
    SeedCategory,
    SeedSubcategory,
)

logger = get_logger(__name__, level=logging.INFO)

DEFAULT_SEED_DATA_CONFIG = DataConfig(type=OutputColumnType.TEXT, params={})

ACTION_FUN_NAMES = ["Greteling", "Vibing", "Magicking", "Noodling"]

EXPERIMENTAL_WARNING = """\
Thanks for trying the 🪄 Magic `DataDesigner` interface! This interface is experimental \
and will likely change in the future. Please let us know your feedback and any issues \
@ https://github.com/gretelai/gretel-python-client/issues/new.
"""


def action_fun_name() -> str:
    """Have a good time 🍻"""
    return random.choice(ACTION_FUN_NAMES)


def seed_to_existing_column(
    seed: Union[SeedCategory, SeedSubcategory]
) -> ExistingColumn:
    """Convert a seed-like object into an ExistingColumn"""
    description = seed.description if seed.description else ""
    return ExistingColumn(
        name=seed.name, description=description, data_config=DEFAULT_SEED_DATA_CONFIG
    )


def gdc_to_existing_column(gdc: GeneratedDataColumn) -> ExistingColumn:
    """Convert a generated data column into an Existing Column"""
    gdc_desc = getattr(gdc, "description", None)
    if gdc_desc is None:
        description = gdc.generation_prompt
    else:
        description = gdc_desc

    return ExistingColumn(
        name=gdc.name, description=description, data_config=gdc.data_config
    )


def generation_config_to_gdc(
    cfg: GenerateColumnFromTemplateConfig,
) -> GeneratedDataColumn:
    return GeneratedDataColumn(
        name=cfg.name,
        generation_prompt=cfg.prompt,
        llm_type=cfg.model_alias,
        data_config=cfg.data_config,
    )


def pprint_column_name(name: str):
    return f"[italic deep_sky_blue1]{name}[/italic deep_sky_blue1]"


def pprint_generation_config(config: GenerateColumnFromTemplateConfig):
    """Pretty print the generation config."""
    console = Console()
    display_group = Group(
        Panel(
            Syntax(
                f'f"{config.prompt}"',
                "python",
                word_wrap=True,
                theme="dracula",
            ),
            title="Generation Prompt Template",
            title_align="left",
        ),
        Panel(
            Pretty(config.data_config), title="Output Data Config", title_align="left"
        ),
    )
    console.print(
        Panel(
            display_group,
            title=f"🪄 Proposed Config for {pprint_column_name(config.name)}",
        )
    )


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
        except json.JSONDecodeError:
            return x

    def render_output(x):
        if syntax is None:
            x = _load_if_json(x)
            if isinstance(x, str):
                return x
            else:
                return Pretty(x)
        else:
            return Syntax(x, syntax)

    panels = [Panel(render_output(item), expand=True) for item in items]

    # Add rows to the grid: slice the panels list into rows.
    for i in range(0, len(panels), columns):
        row_panels = panels[i : i + columns]
        grid.add_row(*row_panels)

    # Wrap the entire grid in an outer Panel.
    return grid


def pprint_outputs(
    preview_samples, name: str, n: int = 6, syntax: Optional[str] = None
):
    """Pretty print generation outputs"""
    last_step_samples = preview_samples.dataset[name].values.tolist()
    console = Console()
    console.print(
        Panel(
            make_panel_grid(last_step_samples[:n], 3, syntax),
            title=f"🎲 Samples of {pprint_column_name(name)}",
        )
    )


## We use a local defined TypeVar in order
## to prevent circular imports with the DataDesigner object.
DataDesignerT = TypeVar("DataDesignerT")


class MagicDataDesignerEditor(Generic[DataDesignerT]):
    dataset_objective: str
    row_entity: str
    dd_state: DataDesignerT
    source_dd_state: DataDesignerT
    _seeds: Optional[CategoricalDataSeeds]
    _modified_columns: list[str]
    model_suite: ModelSuite

    def __init__(
        self,
        dd_state: DataDesignerT,
        *,
        dataset_objective: str = "",
        row_entity: str = "",
        model_suite: ModelSuite = ModelSuite.LLAMA_3_x,
    ):
        self.dataset_objective = dataset_objective
        self.row_entity = row_entity
        self.dd_state = deepcopy(dd_state)  # Local working copy
        self.source_dd_state = dd_state  # Source state
        self.model_suite = model_suite
        self._seeds = None
        self._modified_columns = []

    def reset(self, keep_seeds: bool = False):
        """Start over from scratch."""
        self.dd_state = deepcopy(self.source_dd_state)
        self._modified_columns = []
        if not keep_seeds:
            self._seeds = None

    def save(self):
        """Save current state into the source state."""
        for name in self._modified_columns:
            rich.print(
                f"[bold green]🪄 Column [italic deep_sky_blue1]{name}[/italic deep_sky_blue1] updated in source DataDesigner![/bold green]"
            )
            self.source_dd_state._generated_data_columns[name] = (
                self.dd_state._generated_data_columns[name]
            )

        ## Reset ourselves
        self.reset(keep_seeds=True)

    @property
    def seeds(self):
        """Run the seed generation task and cache the results."""
        if self._seeds is None:
            self._seeds = self.dd_state.run_data_seeds_step(verbose_logging=False)
        return self._seeds

    def _run_task(self, task: Task):
        console = Console()
        column_name = getattr(task.config, "name", "")
        status_string = f"{action_fun_name()} {pprint_column_name(column_name)}"
        with console.status(status_string, spinner="toggle10"):
            output = task.run()
        return output

    def _run_and_display_preview(self, name: str, syntax: Optional[str] = None) -> None:
        outs = self.dd_state.generate_dataset_preview(data_seeds=self.seeds)
        pprint_outputs(outs, name, n=3, syntax=syntax)

    def _single_instruction_call(
        self, task: GenerateColumnConfigFromInstruction, *, preview: bool
    ) -> GenerateColumnFromTemplateConfig:
        """Single call to get a generation config."""
        generation_config = GenerateColumnFromTemplateConfig.model_validate(
            self._run_task(task)
        )
        pprint_generation_config(generation_config)
        name = task.config.name

        self.dd_state._generated_data_columns[name] = generation_config_to_gdc(
            generation_config
        )

        if preview:
            self._run_and_display_preview(
                name,
                syntax=getattr(generation_config.data_config.params, "syntax", None),
            )

        return generation_config

    def _instruction_loop(
        self,
        task: GenerateColumnConfigFromInstruction,
        *,
        interactive: bool,
        preview: bool,
    ) -> Optional[GenerateColumnFromTemplateConfig]:
        """Common call loop for executing the instruction call task."""
        commands = ["accept", "cancel", "retry", "preview", "preview-on", "preview-off"]
        command = "retry"
        terminal_commands = ["accept", "cancel"]
        fail_commands = ["cancel"]
        commands_str = "/".join(commands)

        generation_config = None
        while command not in terminal_commands:
            if command not in commands:
                ## User has requested an edit
                task.config.edit_task = generation_config
                task.config.instruction = command
                generation_config = self._single_instruction_call(task, preview=preview)
            elif command == "preview":
                self._run_and_display_preview(
                    task.config.name,
                    syntax=getattr(
                        generation_config.data_config.params, "syntax", None
                    ),
                )
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
                generation_config = self._single_instruction_call(task, preview=preview)

            if not interactive:
                ## Halt without prompting, mission accomplished.
                break

            command = Prompt.ask(
                f"[bold]\[{commands_str}] or your instructions[/bold]", default="retry"
            )

        if command in fail_commands:
            return None

        return generation_config

    @experimental(message=EXPERIMENTAL_WARNING, emoji="🧪")
    def add_column(
        self,
        name: str,
        description: str,
        *,
        must_depend_on: Optional[list[str]] = None,
        preview: bool = False,
        save: bool = True,
        interactive: bool = True,
    ):
        if name in self.dd_state.all_column_names:
            raise KeyError(
                f"The column {name} already exists in the config. Did you mean to use `edit_column`?"
            )

        if must_depend_on is None:
            must_depend_on = []

        for column_name in must_depend_on:
            if column_name not in self.dd_state.all_column_names:
                raise KeyError(f"Required dependency {column_name} does not exist.")

        task = GenerateColumnConfigFromInstruction(
            name=name,
            instruction=description,
            must_depend_on=must_depend_on,
            existing_columns=self.existing_columns,
            client=self.dd_state._client,
            model_suite=self.model_suite,
        )

        generation_config = self._instruction_loop(
            task, interactive=interactive, preview=preview
        )

        if generation_config is None:
            self.dd_state = deepcopy(self.source_dd_state)
            rich.print(f"🗑️ Trashed {pprint_column_name(name)}")
        elif not save:
            self._modified_columns.append(name)
            rich.print(
                f"🪄 Column {pprint_column_name(name)} buffered! Use .save() to apply later."
            )
        else:
            self._modified_columns.append(name)
            self.save()

        return generation_config

    @experimental(message=EXPERIMENTAL_WARNING, emoji="🧪")
    def edit_column(
        self,
        name: str,
        instruction: str,
        *,
        preview: bool = False,
        save: bool = True,
        interactive: bool = True,
    ):
        if name in self.dd_state.categorical_seed_column_names:
            raise NotImplementedError("Seed editing not yet implemented.")
        elif name not in self.editable_column_names:
            raise KeyError(f"Unknown column name {name}")

        gdc_config = self.dd_state.get_generated_data_column(name)

        existing_config = GenerateColumnFromTemplateConfig(
            name=gdc_config.name,
            prompt=gdc_config.generation_prompt,
            model_alias=gdc_config.llm_type,
            data_config=gdc_config.data_config,
        )

        task = GenerateColumnConfigFromInstruction(
            name=name,
            instruction=instruction,
            existing_columns=self.existing_columns,
            edit_task=existing_config,
            client=self.dd_state._client,
        )

        generation_config = self._instruction_loop(
            task, interactive=interactive, preview=preview
        )

        if generation_config is None:
            self.dd_state = deepcopy(self.source_dd_state)
            rich.print(f"🗑️ Trashed {pprint_column_name(name)}")
        elif not save:
            self._modified_columns.append(name)
            rich.print(
                f"🪄 Column {pprint_column_name(name)} buffered! Use .save() to apply later."
            )
        else:
            self._modified_columns.append(name)
            self.save()

        return generation_config

    @property
    def editable_column_names(self) -> list[str]:
        """List out columns that can be edited with Magick."""
        seed_names = self.dd_state.categorical_seed_column_names

        return [
            name for name in self.dd_state.all_column_names if name not in seed_names
        ]

    def _seeds_state_as_existing_columns(self) -> ExistingColumns:
        """Helper function to recursively unpack seed subcategories."""

        def _to_existing_columns(seed):
            subcategories = getattr(seed, "subcategories", None)
            if subcategories:
                return ExistingColumns(variables=[seed_to_existing_column(seed)]) + sum(
                    _to_existing_columns(ss) for ss in subcategories
                )

            return ExistingColumns(variables=[seed_to_existing_column(seed)])

        return sum(
            _to_existing_columns(ss)
            for ss in self.dd_state.categorical_seed_columns.seed_categories
        )

    def _generated_colums_as_existing_columns(self) -> ExistingColumns:
        """Helper function to grab all generation tasks."""
        return ExistingColumns(
            variables=[
                gdc_to_existing_column(self.dd_state.get_generated_data_column(name))
                for name in self.dd_state.generated_data_column_names
            ]
        )

    @property
    def existing_columns(self) -> ExistingColumns:
        """Reinterpret as existing columns"""
        seed_cols = self._seeds_state_as_existing_columns()
        generation_cols = self._generated_colums_as_existing_columns()
        return seed_cols + generation_cols
