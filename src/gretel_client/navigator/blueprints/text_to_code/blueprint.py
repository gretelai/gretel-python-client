from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from gretel_client.gretel.config_setup import smart_load_yaml
from gretel_client.navigator.blueprints.base import NavigatorBlueprint
from gretel_client.navigator.blueprints.text_to_code.prompt_templates import (
    CODE_PROMPT,
    FIELD_GENERATION_PROMPT,
    TEXT_PROMPT,
)
from gretel_client.navigator.blueprints.text_to_code.utils import display_nl2code_sample
from gretel_client.navigator.tasks import (
    GenerateColumnFromTemplate,
    GenerateSeedValues,
    SampleDataSeeds,
    ValidateCode,
)
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.io import Dataset
from gretel_client.navigator.workflow import NavigatorWorkflow

output_parser_instructions = {
    "pass_through": "* Return only the requested text, without any additional comments or instructions.",
    "json_array": "* Respond only with the list as a valid JSON array.",
}

output_parser_type_map = {
    "str": "pass_through",
    "string": "pass_through",
    "text": "pass_through",
    "json": "json",
    "dict": "json",
    "list": "json_array",
    "json_array": "json_array",
    "code": "extract_code",
}


@dataclass
class DataPreview:
    dataset: Dataset
    contextual_columns: list[dict]
    blueprint_config: dict
    data_seeds: dict

    def display_sample(self, index: Optional[int] = None, **kwargs):
        if index is None:
            record = self.dataset.sample(1).iloc[0]
        else:
            record = self.dataset.loc[index]
        display_nl2code_sample(
            lang=self.blueprint_config["programming_language"],
            record=record,
            contextual_columns=self.contextual_columns,
            **kwargs,
        )


class TextToCodeBlueprint(NavigatorBlueprint):

    def __init__(self, config: Union[str, dict, Path], **session_kwargs):
        self.config = smart_load_yaml(config)
        self.lang = self.config["programming_language"]
        self.workflow = NavigatorWorkflow(**session_kwargs)
        self.task_list = self._build_sequential_task_list()
        self.workflow.add_steps(
            self.workflow.create_steps_from_sequential_tasks(self.task_list)
        )

    def _create_context_template(self, columns: list) -> str:
        return "\n".join(
            [f"    * {c.replace('_', ' ').capitalize()}: {{{c}}}" for c in columns]
        )

    def _create_contextual_column_task(self, field) -> Task:
        output_parser = output_parser_type_map[field["column_type"]]
        generation_type = "text" if field["llm_type"] == "nl" else "code"
        system_prompt = self.config[f"{generation_type}_generation_instructions"]
        return GenerateColumnFromTemplate(
            prompt_template=FIELD_GENERATION_PROMPT.format(
                name=field["name"],
                description=field["description"],
                context=self._create_context_template(field["relevant_columns"]),
                generation_type=generation_type.capitalize(),
                parser_instructions=output_parser_instructions[output_parser],
            ),
            response_column_name=field["name"],
            system_prompt=system_prompt,
            workflow_label=f"{field['name'].replace('_', ' ')}",
            llm_type=field["llm_type"],
            output_parser=output_parser,
        )

    def _build_sequential_task_list(self) -> list[Task]:
        additional_context_columns = []
        for field in self.config.get("additional_contextual_columns", []):
            additional_context_columns.append(
                self._create_contextual_column_task(field)
            )

        generate_text_column = GenerateColumnFromTemplate(
            prompt_template=TEXT_PROMPT.format(
                lang=self.lang,
                context=self._create_context_template(
                    self.config["text_relevant_columns"]
                ),
            ),
            llm_type="nl",
            response_column_name="text",
            system_prompt=self.config["text_generation_instructions"],
            workflow_label="text prompt",
        )

        generate_code_column = GenerateColumnFromTemplate(
            prompt_template=CODE_PROMPT.format(
                lang=self.lang,
                context=self._create_context_template(
                    self.config["code_relevant_columns"]
                ),
            ),
            llm_type="nl",
            response_column_name="code",
            system_prompt=self.config["code_generation_instructions"],
            workflow_label="code prompt",
            output_parser="extract_code",
        )

        return [
            GenerateSeedValues(**self.config["seed_generation"]),
            SampleDataSeeds(),
            *additional_context_columns,
            generate_text_column,
            generate_code_column,
            ValidateCode("python"),
        ]

    def generate_dataset_preview(self) -> DataPreview:
        results = self.workflow.generate_dataset_preview()

        seeds = {}
        for s in results.auxiliary_outputs[0]["seed_columns"]:
            seeds[s["name"]] = s["starting_values"] + s["generated_values"]

        additional_context = self.config.get("additional_contextual_columns", [])
        context_cols = [
            s["name"] for s in self.config["seed_generation"]["seed_columns"]
        ]
        return DataPreview(
            dataset=results.dataset,
            contextual_columns=context_cols
            + [field["name"] for field in additional_context],
            blueprint_config=self.config,
            data_seeds=seeds,
        )

    def submit_batch_job(
        self, num_records: int, project_name: Optional[str] = None
    ) -> None:
        self.workflow.submit_batch_job(
            num_records=num_records, project_name=project_name
        )
