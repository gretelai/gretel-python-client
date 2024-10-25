import logging

from pathlib import Path
from typing import Literal, Optional, Union

from rich import print as rich_print
from rich.pretty import pprint as rich_pprint
from typing_extensions import Self

from gretel_client.gretel.config_setup import smart_load_yaml
from gretel_client.navigator.data_designer.data_column import DataColumn
from gretel_client.navigator.data_designer.prompt_templates import (
    get_prompt_template_keywords,
)
from gretel_client.navigator.log import get_logger
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.generate.generate_seed_values import (
    GenerateSeedValues,
)
from gretel_client.navigator.tasks.load_data_seeds import LoadDataSeeds
from gretel_client.navigator.tasks.seed.sample_data_seeds import SampleDataSeeds
from gretel_client.navigator.tasks.types import DataSeedColumn, NestedDataSeedColumn
from gretel_client.navigator.tasks.validate.validate_code import ValidateCode
from gretel_client.navigator.workflow import NavigatorWorkflow, WorkflowResults

logger = get_logger(__name__, level=logging.INFO)


class DataDesigner:

    def __init__(
        self,
        *,
        dataset_description: Optional[str] = None,
        special_system_instructions: Optional[str] = None,
        **session_kwargs,
    ):
        self._data_columns = {}
        self._seed_columns = {}
        self._data_validators = []
        self._nested_seed_names = set()
        self._workflow_steps = []
        self._seed_columns_need_generation = False
        self._final_seed_columns = None

        self.dataset_description = dataset_description
        self.special_system_instructions = special_system_instructions

        self.workflow = NavigatorWorkflow(**session_kwargs)

    @property
    def column_names(self) -> list[str]:
        return (
            self._seed_column_names
            + list(self._nested_seed_names)
            + self._data_column_names
        )

    @property
    def _data_column_names(self) -> list[str]:
        return list(self._data_columns.keys())

    @property
    def _seed_column_names(self) -> list[str]:
        return list(self._seed_columns.keys())

    @classmethod
    def from_config(cls, config: Union[dict, str, Path], **session_kwargs) -> Self:
        config = smart_load_yaml(config)

        designer = cls(
            dataset_description=config.get("dataset_description"),
            special_system_instructions=config.get("special_system_instructions"),
            **session_kwargs,
        )

        for seed_column in config.get("seed_columns", []):
            designer.add_seed_column(**seed_column)
            logger.debug(f"🌱 Adding seed column: {seed_column['name']}")

        for data_column in config.get("data_columns", []):
            designer.add_data_column(**data_column)
            logger.debug(f"💽 Adding data column: {data_column['name']}")

        if len(designer.column_names) == 0:
            raise ValueError("No seed or data columns were defined in the config.")

        for settings in config.get("data_validators", []):
            s = settings.copy()
            validator = s.pop("validator")
            if validator == "code":
                logger.debug(f"🔍 Adding code validation: {settings}")
                designer._data_validators.append(
                    ValidateCode(client=designer.workflow._client, **s)
                )
            else:
                raise ValueError(f"Unknown validator type: {validator}")

        designer._config = config

        return designer

    def _create_sequential_task_list(self) -> list[Task]:
        task_list = []
        if not self._seed_columns_need_generation:
            task_list.append(
                LoadDataSeeds(
                    seed_columns=self._final_seed_columns, client=self.workflow._client
                )
            )
        else:
            task_list.append(
                GenerateSeedValues(
                    seed_columns=list(self._seed_columns.values()),
                    dataset_context=self.dataset_description,
                    client=self.workflow._client,
                )
            )
        task_list.append(SampleDataSeeds(client=self.workflow._client))
        for column in self._data_columns.values():
            task = column.to_generation_task(
                self.special_system_instructions, client=self.workflow._client
            )
            task_list.append(task)

        for validator in self._data_validators:
            task_list.append(validator)

        return task_list

    def _validate_data_column_inputs(
        self,
        name: str,
        description: str,
        relevant_columns: Optional[list[str]] = None,
        specific_instructions: Optional[str] = None,
    ) -> tuple[str, str, list[str], str]:
        if name in self._data_columns:
            raise ValueError(f"Column name `{name}` already exists.")
        if name in self._seed_columns:
            raise ValueError(f"Column name `{name}` already exists as a seed column.")

        specific_instructions = specific_instructions or ""
        for n, v in zip(
            ["description", "specific_instructions"],
            [description, specific_instructions],
        ):
            template_kwargs = get_prompt_template_keywords(v)
            if not template_kwargs.issubset(self.column_names):
                raise ValueError(
                    f"The `{n}` field of `{name}`contains template keywords that "
                    "are not available as columns.\n"
                    f"* Template keywords found in `{n}`: {template_kwargs}\n"
                    f"* Available seed columns: {self._seed_column_names}\n"
                    f"* Available data columns: {self._data_column_names}"
                )

        relevant_columns = relevant_columns or []
        if any(col not in self.column_names for col in relevant_columns):
            raise ValueError(
                f"The `relevant_columns` field of `{name}` is not configured correctly. "
                "Relevant columns must be added *before* the column that references them.\n"
                f"* Available seed columns: {self._seed_column_names}\n"
                f"* Available data columns: {self._data_column_names}\n"
            )

        return name, description, relevant_columns, specific_instructions

    def generate_seed_values(self, force_generation: bool = False) -> None:
        if len(self._seed_columns) == 0:
            raise ValueError("No seed columns have been defined.")

        if not self._seed_columns_need_generation and not force_generation:
            logger.warning(
                "Seed columns do not require generation. If you want to force "
                "generation, set `force_generation=True`."
            )
            return

        task = GenerateSeedValues(
            seed_columns=list(self._seed_columns.values()),
            dataset_context=self.dataset_description,
            client=self.workflow._client,
        )

        logger.info("🦜 Generating seed values")
        self._final_seed_columns = task.run()
        self._seed_columns_need_generation = self._final_seed_columns is None

    def inspect_data_seeds(self) -> None:
        if self._final_seed_columns is None:
            logger.warning(
                "No seed columns have been generated. Run `generate_seed_values()` first."
            )
        else:
            columns_to_print = [
                "name",
                "description",
                "values",
                "generated_values",
                "num_values_to_generate",
                "nested_data_seeds",
            ]
            rich_print("-" * 80 + "\n🌱 Data Seed Columns\n" + "-" * 80)
            for seed in self._final_seed_columns["seed_columns"]:
                rich_pprint(
                    {k: v for k, v in seed.items() if k in columns_to_print},
                    indent_guides=False,
                )
            print("-" * 80)

    def reset_seed_values(self) -> None:
        if self._final_seed_columns is not None:
            logger.info("🔥 Resetting seed values")
            self._final_seed_columns = None
            self._seed_columns_need_generation = True
        else:
            logger.warning("No seed columns have been generated.")

    def add_seed_column(
        self,
        name: str,
        *,
        description: Optional[str] = None,
        values: Optional[list[Union[str, int, float]]] = None,
        weights: Optional[list[float]] = None,
        num_values_to_generate: Optional[int] = None,
        nested_data_seeds: Optional[
            Union[list[NestedDataSeedColumn], list[dict]]
        ] = None,
    ) -> None:
        if len(self._data_columns) > 0:
            raise ValueError(
                "Seed columns must be added *before* data columns.\n"
                f"-> Current data columns: {self._data_column_names}"
            )
        if num_values_to_generate is None and values is None:
            raise ValueError(
                "You must provide *at least* one of `values` or `num_values_to_generate`."
            )
        if name in self._seed_column_names:
            raise ValueError(f"Seed column name `{name}` already exists.")

        if len(nested_data_seeds or []) > 0:
            for seed in nested_data_seeds:
                if isinstance(seed, dict):
                    seed = NestedDataSeedColumn(**seed)
                if seed.name in self._seed_column_names:
                    raise ValueError(f"Seed column name `{seed.name}` already exists.")
                self._nested_seed_names |= {seed.name}
                self._seed_columns_need_generation = True

        if num_values_to_generate is not None and num_values_to_generate > 0:
            self._seed_columns_need_generation = True

        self._seed_columns[name] = DataSeedColumn(
            name=name,
            description=description,
            values=values or [],
            weights=weights or [],
            num_values_to_generate=num_values_to_generate,
            nested_data_seeds=nested_data_seeds or [],
        )

    def add_data_column(
        self,
        name: str,
        *,
        description: str,
        output_type: Literal["text", "dict", "list", "code"] = "text",
        relevant_columns: Optional[list[str]] = None,
        specific_instructions: Optional[str] = None,
        llm_type: Literal["nl", "code"] = "nl",
    ) -> None:
        name, description, relevant_columns, specific_instructions = (
            self._validate_data_column_inputs(
                name, description, relevant_columns, specific_instructions
            )
        )
        relevant_columns = relevant_columns or self._seed_column_names
        self._data_columns[name] = DataColumn(
            name=name,
            description=description,
            output_type=output_type,
            relevant_columns=relevant_columns or [],
            specific_instructions=specific_instructions or "",
            llm_type=llm_type,
        )

    def get_seed_column(self, name: str) -> DataSeedColumn:
        return self._seed_columns[name]

    def get_data_column(self, name: str) -> DataColumn:
        return self._data_columns[name]

    def generate_dataset_preview(
        self, *, verbose_logging: bool = False
    ) -> WorkflowResults:
        self.workflow.reset_steps()
        task_list = self._create_sequential_task_list()
        steps = self.workflow.create_steps_from_sequential_tasks(task_list)
        self._workflow_step_names = [s.name for s in steps]
        self.workflow.add_steps(steps)
        results = self.workflow.generate_dataset_preview(
            verbose_logging=verbose_logging
        )
        seeds = [
            v
            for k, v in results.outputs_by_step.items()
            if "load-data-seeds" in k or "generate-seed-values" in k
        ]
        if len(seeds) > 0:
            self._final_seed_columns = seeds[0]
            self._seed_columns_need_generation = False
        return results

    def __repr__(self):
        seed_columns = [
            (
                name
                if len(s.nested_data_seeds) == 0
                else f"{name}:{','.join([n.name for n in s.nested_data_seeds])}"
            )
            for name, s in self._seed_columns.items()
        ]

        validators = (
            f"    validators: {[v.name for v in self._data_validators]}\n"
            if len(self._data_validators) > 0
            else ""
        )

        return (
            f"{self.__class__.__name__}(\n"
            f"    seed_columns: {seed_columns}\n"
            f"    data_columns: {self._data_column_names}\n"
            f"{validators}"
            ")"
        )
