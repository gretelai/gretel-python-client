import json
import logging

from collections import defaultdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, Union

import requests
import yaml

from typing_extensions import Self

from gretel_client.config import ClientConfig
from gretel_client.gretel.config_setup import smart_load_yaml
from gretel_client.navigator.client.interface import TaskOutput
from gretel_client.navigator.client.utils import get_navigator_client
from gretel_client.navigator.data_designer.data_column import GeneratedDataColumn
from gretel_client.navigator.data_designer.prompt_templates import (
    assert_valid_template,
    get_prompt_template_keywords,
)
from gretel_client.navigator.data_designer.viz_tools import (
    display_preview_evaluation_summary,
)
from gretel_client.navigator.log import get_logger
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.constants import PREVIEW_NUM_RECORDS
from gretel_client.navigator.tasks.evaluate_dataset import EvaluateDataset
from gretel_client.navigator.tasks.generate.generate_seed_category_values import (
    GenerateSeedCategoryValues,
)
from gretel_client.navigator.tasks.io import Dataset
from gretel_client.navigator.tasks.judge_with_llm import JudgeWithLLM
from gretel_client.navigator.tasks.load_data_seeds import LoadDataSeeds
from gretel_client.navigator.tasks.seed.sample_data_seeds import SampleDataSeeds
from gretel_client.navigator.tasks.types import (
    CategoricalDataSeeds,
    check_model_suite,
    CodeLang,
    DataConfig,
    DEFAULT_MODEL_SUITE,
    EvaluationType,
    LLMJudgePromptTemplateType,
    LLMType,
    ModelSuite,
    SeedCategory,
    SeedSubcategory,
    SQL_DIALECTS,
    ValidatorType,
)
from gretel_client.navigator.tasks.validate.validate_code import (
    VALIDATE_PYTHON_COLUMN_SUFFIXES,
    VALIDATE_SQL_COLUMN_SUFFIXES,
    ValidateCode,
)
from gretel_client.navigator.workflow import (
    DataDesignerBatchJob,
    DataDesignerWorkflow,
    DataSpec,
    PreviewResults,
    Step,
)

logger = get_logger(__name__, level=logging.INFO)


class DataDesigner:
    """High-level interface for designing synthetic data generation workflows with Gretel Navigator.

    The DataDesigner class streamlines the process of building synthetic datasets using Gretel
    Navigator's Workflows execution framework. It provides a declarative config framework for
    defining categorical data seeds, generated data columns, and data validators, which are used
    to assemble a scalable synthetic data generation workflow.

    Args:
        model_suite: The model suite to use for generating synthetic data. Defaults to the
            apache-2.0 licensed model suite.
        session: Optional Gretel session configuration object. If not provided, the session will be
            configured based on the provided session_kwargs or cached session configuration.
        special_system_instructions: Optional instructions for the system to follow when generating
            the dataset. These instructions will be added to the system prompts.
        **session_kwargs: kwargs for your Gretel session. See options below.

    Allowed session keyword arguments:
        api_key (str): Your Gretel API key. If set to "prompt" and no API key
            is found on the system, you will be prompted for the key.
        endpoint (str): Specifies the Gretel API endpoint. This must be a fully
            qualified URL. The default is "https://api.gretel.cloud".
        default_runner (str): Specifies the runner mode. Must be one of "cloud",
            "local", "manual", or "hybrid". The default is "cloud".
        artifact_endpoint (str): Specifies the endpoint for project and model
            artifacts. Defaults to "cloud" for running in Gretel Cloud. If
            working in hybrid mode, set to the URL of your artifact storage bucket.
        cache (str): Valid options are "yes" or "no". If set to "no", the session
            configuration will not be written to disk. If set to "yes", the
            session configuration will be written to disk only if one doesn't
            already exist. The default is "no".
        validate (bool): If `True`, will validate the login credentials at
            instantiation. The default is `False`.
        clear (bool): If `True`, existing Gretel credentials will be removed.
            The default is `False.`
    """

    @staticmethod
    def fetch_dataset(
        workflow_run_id: str,
        *,
        wait_for_completion: bool = True,
        session: Optional[ClientConfig] = None,
        **session_kwargs,
    ) -> Dataset:
        """Utility method to fetch the synthetic dataset generated in a previously run batch job.

        Args:
            workflow_run_id: Unique id of the batch job.
            wait_for_completion: Boolean to indicate to wait for the the batch job to complete
                if it is still running. Defaults to True.
            session: Optional Gretel session configuration object. If not provided, the session
                will be configured based on the provided session_kwargs or cached session
                configuration.
            **session_kwargs: kwargs for your Gretel session. See options in the class
                docstring above.
        """
        batch_job = DataDesignerBatchJob(
            workflow_run_id=workflow_run_id,
            client=get_navigator_client(session=session, **session_kwargs),
        )
        return batch_job.fetch_dataset(wait_for_completion=wait_for_completion)

    @staticmethod
    def download_evaluation_report(
        workflow_run_id: str,
        *,
        wait_for_completion: bool = True,
        output_dir: Union[str, Path] = Path("."),
        session: Optional[ClientConfig] = None,
        **session_kwargs,
    ) -> None:
        """Utility method to download the evaluation report generated in a previously run batch job.

        Args:
            workflow_run_id: Unique id of the batch job.
            wait_for_completion: Boolean to indicate to wait for the the batch job to complete
                if it is still running. Defaults to True.
            outpu_dir: A string or a Path denoting the local directory where the report should be saved.
            session: Optional Gretel session configuration object. If not provided, the session
                will be configured based on the provided session_kwargs or cached session
                configuration.
            **session_kwargs: kwargs for your Gretel session. See options in the class
                docstring above.
        """
        batch_job = DataDesignerBatchJob(
            workflow_run_id=workflow_run_id,
            client=get_navigator_client(session=session, **session_kwargs),
        )
        return batch_job.download_evaluation_report(
            wait_for_completion=wait_for_completion, output_dir=output_dir
        )

    @staticmethod
    def fetch_step_output(
        workflow_run_id: str,
        *,
        step_name: str,
        wait_for_completion: bool = True,
        session: Optional[ClientConfig] = None,
        **session_kwargs,
    ) -> TaskOutput:
        """Utility method to fetch the output from a specific step in a previously run batch job.

        Args:
            workflow_run_id: Unique id of the batch job.
            step_name: The name of the step.
            wait_for_completion: Boolean to indicate to wait for the the batch job to complete
                if it is still running. Defaults to True.
            session: Optional Gretel session configuration object. If not provided, the session
                will be configured based on the provided session_kwargs or cached session
                configuration.
            **session_kwargs: kwargs for your Gretel session. See options in the class
                docstring above.
        """
        batch_job = DataDesignerBatchJob(
            workflow_run_id=workflow_run_id,
            client=get_navigator_client(session=session, **session_kwargs),
        )
        return batch_job.fetch_step_output(
            step_name=step_name,
            wait_for_completion=wait_for_completion,
        )

    @classmethod
    def from_config(
        cls,
        config: Union[dict, str, Path],
        session: Optional[ClientConfig] = None,
        **session_kwargs,
    ) -> Self:
        """Instantiate a DataDesigner instance from a YAML configuration str, dict, or file.

        Args:
            config: A YAML configuration file, dict, or string.
            **kwargs: Additional keyword arguments to pass to the DataDesigner constructor.

        Returns:
            An instance of DataDesigner configured with the settings from the provided YAML config.
        """
        if isinstance(config, str) and (
            config.startswith("https://gretel")
            or config.startswith("https://raw.githubusercontent.com/gretelai")
        ):
            config = requests.get(config).content.decode("utf-8")
        config = smart_load_yaml(config)
        dd = cls(
            special_system_instructions=config.get("special_system_instructions"),
            model_suite=config.get("model_suite", DEFAULT_MODEL_SUITE),
            session=session,
            **session_kwargs,
        )

        if "categorical_seed_columns" not in config:
            raise ValueError(
                "No categorical seed columns were defined in the config. At least one seed "
                "category must be defined in the `categorical_seed_columns` field."
            )

        for seed_category in config.get("categorical_seed_columns", []):
            dd.add_categorical_seed_column(**seed_category)
            logger.debug(f"🌱 Adding seed category: {seed_category['name']}")

        for data_column in config.get("generated_data_columns", []):
            dd.add_generated_data_column(**data_column)
            logger.debug(f"💽 Adding data column: {data_column['name']}")

        if len(dd.all_column_names) == 0:
            raise ValueError("No seed or data columns were defined in the config.")

        # Post processors are applied after the data generation process
        # Currently, we our post processors are specific to text-to-code,
        # but this will become more general in the future.
        if post_processors := config.get("post_processors"):
            code_lang = None
            eval_type = None
            for processor in post_processors:
                if "validator" in processor:
                    dd.add_validator(
                        validator=ValidatorType(processor["validator"]),
                        **processor["settings"],
                    )
                    code_lang = processor["settings"].get("code_lang")
                elif "evaluator" in processor:
                    settings = processor["settings"].copy()
                    if processor["evaluator"] in list(LLMJudgePromptTemplateType):
                        settings["instruction_column_name"] = settings.pop(
                            "text_column"
                        )
                        settings["response_column_name"] = settings.pop("code_column")
                        settings["context_column_name"] = settings.pop(
                            "context_column", None
                        )
                        eval_type = LLMJudgePromptTemplateType(
                            processor["evaluator"]
                        ).value
                    dd.add_evaluator(eval_type=processor["evaluator"], **settings)
                if code_lang and eval_type:
                    if (code_lang in SQL_DIALECTS and eval_type != "text_to_sql") or (
                        code_lang == "python" and eval_type != "text_to_python"
                    ):
                        raise ValueError(
                            f"The `{code_lang}` code validator is not compatible with "
                            f"the `{eval_type}` evaluator. Please ensure the code language "
                            "of the validator and evaluator are compatible."
                        )

        dd._config = config

        return dd

    def __init__(
        self,
        *,
        model_suite: ModelSuite = DEFAULT_MODEL_SUITE,
        special_system_instructions: Optional[str] = None,
        session: Optional[ClientConfig] = None,
        **session_kwargs,
    ):
        logger.info(f"🦜 Using {model_suite} model suite")
        self._client = get_navigator_client(session=session, **session_kwargs)
        self._seed_categories: dict[str, list[SeedCategory]] = {}
        self._seed_subcategory_names = defaultdict(list)
        self._generated_data_columns: dict[str, list[GeneratedDataColumn]] = {}
        self._validators: dict[str, list[Task]] = {}
        self._evaluators: dict[str, Task] = {}
        self._eval_type: Optional[str] = None
        self._workflow_id: Optional[str] = None
        self._project_id: Optional[str] = None

        self.special_system_instructions = special_system_instructions
        datetime_label = datetime.now().isoformat(timespec="seconds")
        self._workflow_kwargs = {
            "model_suite": check_model_suite(model_suite),
            "client": self._client,
            "workflow_name": f"{self.__class__.__name__}-{datetime_label}",
        }

    @property
    def categorical_seed_column_names(self) -> list[str]:
        """Return a list of the names of the seed columns, including subcategories."""
        return self._seed_category_names + [
            s for ss in self._seed_subcategory_names.values() for s in ss
        ]

    @property
    def generated_data_column_names(self) -> list[str]:
        """Return a list of the names of the data columns (note order matters)."""
        return list(self._generated_data_columns.keys())

    @property
    def all_column_names(self) -> list[str]:
        """Return a list of all seed (including seed subcategories) and data column names."""
        return self.categorical_seed_column_names + self.generated_data_column_names

    @property
    def has_seed_categories(self) -> bool:
        return len(self._seed_categories) > 0

    @property
    def categorical_seed_columns(self) -> CategoricalDataSeeds:
        """Return a CategoricalDataSeeds instance that contains the seed categories."""
        if self.has_seed_categories:
            return CategoricalDataSeeds(
                seed_categories=list(self._seed_categories.values())
            )
        return CategoricalDataSeeds(seed_categories=[])

    @property
    def _seed_category_names(self) -> list[str]:
        """Return a list of the names of parent seed categories."""
        return list(self._seed_categories.keys())

    def add_generated_data_column(
        self,
        name: str,
        *,
        generation_prompt: str,
        columns_to_list_in_prompt: Optional[list[str]] = None,
        llm_type: LLMType = LLMType.NATURAL_LANGUAGE,
        data_config: Optional[dict] = None,
    ) -> None:
        """Add a generated data column to the data design.

        Generated data columns are fully generated by an LLM using the provided generation prompt.

        Data Configuration:
            The data configuration is used to specify details about the data generation, parsing, and
            validation procedure. The data configuation specifies a type and a set of parameters specific
            to that type.

                    ```
                    data_config = {"type": "type-name", "params": { "key": "value", ...}
                    ```

            * **Free-text.** Free-text generation implies no processing or validation to LLM outputs, and the
                text itself is not parsed.

                    ```
                    data_config = {"type": "text"}
                    ```

            * **Code.** Code generation will ensure that generated data is code of the specified language.

                    ```
                    data_config = {"type": "code", "params": {"syntax": "...language..."}}
                    ```

            * **Structured Data.** This data configuration will ensure that a JSON schema is followed when
                generating data. The returned data is ensured to match the provided schema. Structured
                data is saved in the resulting dataset as strings of serialized JSON.

                    ```
                    data_config = {"type": "structured", "params": {"model": PydanticBaseModelType}
                    data_config = {"type": "structured", "params": {"json_schema": dict}
                    ```

                The structured response format can be specified using either a Pydantic ``BaseModel``
                type, or by a python dictionary matching a valid
                [JSON schema](https://json-schema.org/draft/2020-12/json-schema-core).


        Args:
            name: The name of the data column.
            generation_prompt: The prompt that will be used to generate the data column. The prompt and can
                contain template keywords that reference seed columns or other existing data columns.
            columns_to_list_in_prompt: List of seed and/or data columns to list as context in the generation prompt.
            llm_type: LLM type for generation of the column. Must be one of ["nl", "code", "judge"].
            data_config: A dictionary configuration specifying how the data in the generated column should
                be interpreted. Augments generation, parsing, and validation. See docstring notes above
                for more information on supported data types. If ``None``, defaults to free-text data configuration.
                Default: ``None``.
        """
        if data_config is None:
            data_config = {"type": "text"}

        name, generation_prompt, columns_to_list_in_prompt = (
            self._validate_generated_data_column_inputs(
                name, generation_prompt, columns_to_list_in_prompt
            )
        )
        self._generated_data_columns[name] = GeneratedDataColumn(
            name=name,
            generation_prompt=generation_prompt,
            columns_to_list_in_prompt=columns_to_list_in_prompt,
            llm_type=llm_type,
            data_config=DataConfig.model_validate(data_config),
        )

    def add_categorical_seed_column(
        self,
        name: str,
        *,
        description: Optional[str] = None,
        values: Optional[list[Union[str, int, float]]] = None,
        weights: Optional[list[float]] = None,
        num_new_values_to_generate: Optional[int] = None,
        subcategories: Optional[Union[list[SeedSubcategory], list[dict]]] = None,
        **kwargs,
    ) -> None:
        """Add a seed category to the data design.

        All seed categories must be added *before* any generated data columns are added. This is to enable
        validation of any keyword arguments in the generation prompts, which can only reference existing
        seed and data columns.

        A seed category is a categorical column with values that can be user-provided or generated
        by an LLM using the GenerateSeedCategoryValues Task. The purpose of categorical data
        seeds is to steer the synthetic data generation process, injecting diversity along axes
        that are important to the user.

        Args:
            name: The name of the seed category.
            description: A clear and specific description of the seed category.
            values: A list of values for the category.
            weights: A list of weights for the values. If None, all values will have equal weight.
                weights must be the same length as values.
            num_new_values_to_generate: The number of new category values to generate.
                If None, only the provided values will be used. You must provide *at least* one
                of `values` or `num_new_values_to_generate`. If you provide both, then `values`
                will be used as examples during generation.
            subcategories: Subcategories for the seed category. Each subcategory must be a
                SeedSubcategory instance or a dictionary with the same fields as a parent
                seed category *except* for `weights` and `subcategories`.
        """
        if num_new_values_to_generate is None and values is None:
            raise ValueError(
                "You must provide *at least* one of `values` or `num_new_values_to_generate`."
            )

        if len(subcategories or []) > 0:
            for seed in subcategories:
                if isinstance(seed, dict):
                    seed = SeedSubcategory(**seed)
                if seed.name in self._seed_category_names:
                    raise ValueError(f"Seed category `{seed.name}` already exists.")
                if seed.name not in self._seed_subcategory_names[name]:
                    self._seed_subcategory_names[name].append(seed.name)

        weights = weights or []
        if len(weights) > 0 and len(weights) != len(values):
            raise ValueError(
                "The number of weights must be the same as the number of values."
            )

        self._seed_categories[name] = SeedCategory(
            name=name,
            description=description,
            values=values or [],
            weights=weights or [],
            num_new_values_to_generate=num_new_values_to_generate,
            subcategories=subcategories or [],
            **kwargs,
        )

    def add_validator(self, validator: ValidatorType, **settings) -> None:
        """Add a data validator to the data design.

        Args:
            validator: The type of validator. Currently, only "code" is supported.
            **settings: Validator-specific settings. For code validation, you must provide
                `code_lang` and `code_columns`.
        """
        if validator == ValidatorType.CODE:
            if "code_lang" not in settings:
                raise ValueError("You must provide `code_lang` for code validation.")
            CodeLang.validate(settings["code_lang"])
            if "code_columns" not in settings:
                raise ValueError("You must provide `code_columns` for code validation.")
            if not isinstance(settings["code_columns"], list):
                raise ValueError(
                    "`code_columns` must be a list of column names. "
                    f"You provided: {settings['code_columns']}"
                )
            if not set(settings["code_columns"]).issubset(self.all_column_names):
                raise ValueError(
                    "`code_columns` contains columns that have not been defined."
                    f"\n* Available columns: {self.all_column_names}"
                )
            self._validators[ValidatorType(validator).value] = ValidateCode(
                client=self._client, **settings
            )
        else:
            raise ValueError(f"Unknown validator type: {validator}")

    def add_evaluator(
        self, eval_type: Union[EvaluationType, LLMJudgePromptTemplateType], **settings
    ) -> None:
        """Add a dataset evaluation task to the data design.

        Args:
            eval_type: The type of evaluation to perform.
            **settings: Evaluation-specific settings.
        """
        llm_judge_column = ""
        if eval_type in list(LLMJudgePromptTemplateType):
            if "instruction_column_name" not in settings:
                raise ValueError(
                    f"You must provide `instruction_column_name` for {eval_type} evaluation."
                )
            if "response_column_name" not in settings:
                raise ValueError(
                    f"You must provide `response_column_name` for {eval_type} evaluation."
                )
            instruction_column_name = settings.get("instruction_column_name")
            response_column_name = settings.get("response_column_name")
            self._evaluators["judge_with_llm"] = JudgeWithLLM(
                judge_template_type=eval_type,
                instruction_column_name=instruction_column_name,
                response_column_name=response_column_name,
                context_column_name=settings.get("context_column_name"),
                num_samples_to_judge=settings.get("num_samples_to_judge", 100),
                client=self._client,
            )
            eval_type = LLMJudgePromptTemplateType(eval_type).value
            llm_judge_column = f"{eval_type}_llm_judge_results"

        elif eval_type in list(EvaluationType):
            eval_type = EvaluationType(eval_type).value

        else:
            raise ValueError(f"Unknown evaluation type: {eval_type}")

        self._evaluators["evaluate_dataset"] = EvaluateDataset(
            seed_columns=self.categorical_seed_column_names,
            ordered_list_like_columns=settings.get("ordered_list_like_columns", []),
            other_list_like_columns=settings.get("list_like_columns", []),
            llm_judge_column=settings.get("llm_judge_column", llm_judge_column),
            columns_to_ignore=settings.get("columns_to_ignore", []),
            client=self._client,
        )
        self._eval_type = eval_type

    def get_generated_data_column(self, name: str) -> GeneratedDataColumn:
        """Get a data column by name."""
        return self._generated_data_columns[name]

    def get_data_spec(
        self, data_seeds: Optional[CategoricalDataSeeds] = None
    ) -> DataSpec:
        """Return a DataSpec instance that defines the schema and other relevant info."""
        code_lang = None
        code_columns = []
        validation_columns = []
        llm_judge_column_name = None
        for validator in self._validators.values():
            if validator.name == "validate_code":
                column_suffix = (
                    VALIDATE_SQL_COLUMN_SUFFIXES
                    if validator.config.code_lang in SQL_DIALECTS
                    else VALIDATE_PYTHON_COLUMN_SUFFIXES
                )
                code_lang = validator.config.code_lang
                code_columns.extend(validator.config.code_columns)
                for col in code_columns:
                    for prefix in column_suffix:
                        validation_columns.append(f"{col}{prefix}")
                break
        if self._eval_type in list(LLMJudgePromptTemplateType):
            llm_judge_column_name = f"{self._eval_type}_llm_judge_results"

        seed_category_names = (
            self._seed_category_names
            if data_seeds is None
            else [s.name for s in data_seeds.seed_categories]
        )

        seed_subcategory_names = (
            dict(self._seed_subcategory_names)
            if data_seeds is None
            else {
                s.name: [ss.name for ss in s.subcategories]
                for s in data_seeds.seed_categories
            }
        )

        return DataSpec(
            seed_category_names=seed_category_names,
            seed_subcategory_names=seed_subcategory_names,
            data_column_names=self.generated_data_column_names,
            validation_column_names=validation_columns,
            code_column_names=code_columns,
            code_lang=code_lang,
            eval_type=self._eval_type,
            llm_judge_column_name=llm_judge_column_name,
        )

    def export_as_workflow_config(
        self,
        path: Optional[Union[str, Path]] = None,
        *,
        num_records: Optional[int] = None,
        dataset_context: Optional[str] = None,
        data_seeds: Optional[CategoricalDataSeeds] = None,
        **kwargs,
    ) -> Union[dict, None]:
        """Export the data design as a Navigator workflow configuration.

        The path can be JSON or YAML. If no path is provided, the configuration
        will be returned as a dict.

        Args:
            path: Optional JSON or YAML path to save the workflow configuration to.
            num_records: Number of records to be generated.
            dataset_context: Context for the dataset to be used in the seed generation task.
            data_seeds: Data seeds to use in place of what was defined
                in the DataDesigner configuration. This is useful if, for example, you
                generated category values using `generate_seed_category_values` and want to
                use those specific values in the workflow configuration.

        Returns:
            If no path is provided, the configuration will be returned as a dict.
        """
        workflow = DataDesignerWorkflow(**self._workflow_kwargs)

        steps, _ = self._create_workflow_steps(
            num_records=num_records,
            dataset_context=dataset_context,
            data_seeds=data_seeds,
            **kwargs,
        )
        workflow.add_steps(steps)
        config = workflow.to_dict()
        config["globals"]["num_records"] = num_records

        # Subclasses of Enum (which we often use for categorical options in configs)
        # are _not_ automatically converted to their desired string representations
        # by pyyaml. We need to register a representer for all subclasses of Enum
        # to be able to catch all these cases at write-time.
        yaml.add_multi_representer(
            Enum,
            lambda dumper, e: dumper.represent_scalar(
                "tag:yaml.org,2002:str", str(e.value)
            ),
        )

        if path is None:
            return config
        else:
            path = Path(path)
            if path.suffix.lower() in [".yaml", ".yml"]:
                with open(path, "w") as f:
                    yaml.dump(config, f)
            elif path.suffix.lower() == ".json":
                with open(path, "w") as f:
                    json.dump(config, f)
            else:
                raise ValueError(
                    "The file extension must be on of .yaml, .yml, or .json."
                    f"You provided: {path.suffix}"
                )

    def ingest_categorical_data_seeds(
        self, data_seeds: Union[dict, CategoricalDataSeeds, str, Path]
    ) -> None:
        """Ingest pre-generated data seeds into the data design.

        Args:
            data_seeds: Ingest categorical data seeds from a CategoricalDataSeeds instance, dict,
                or YAML/JSON file.
        """
        if isinstance(data_seeds, (str, Path)):
            data_seeds = Path(data_seeds)
            if data_seeds.suffix.lower() in [".yaml", ".yml"]:
                with open(data_seeds, "r") as f:
                    data_seeds = yaml.safe_load(f)
            elif data_seeds.suffix.lower() == ".json":
                with open(data_seeds, "r") as f:
                    data_seeds = json.load(f)
            else:
                raise ValueError(
                    "The file extension must be one of .yaml, .yml, or .json. "
                    f"You provided: {data_seeds.suffix}"
                )
        data_seeds = CategoricalDataSeeds.model_validate(data_seeds)
        logger.info("🌱 Ingesting categorical data seeds into data design")
        for seed in data_seeds.seed_categories:
            try:
                self.add_categorical_seed_column(**seed.model_dump())
            except NotImplementedError:
                raise NotImplementedError(
                    "Ingesting data seeds is not supported for the "
                    f"{self.__class__.__name__} subclass."
                )

    def run_data_seeds_step(
        self,
        *,
        dataset_context: Optional[str] = None,
        verbose_logging: bool = False,
        **kwargs,
    ) -> CategoricalDataSeeds:
        """Run workflow step that generates / extracts / defines data seeds.

        Args:
            dataset_context: Context for the dataset to be used in the seed value generation task.
            verbose_logging: If True, additional logging will be displayed during execution.
        """
        task = self._get_data_seeds_task(dataset_context=dataset_context, **kwargs)
        workflow = DataDesignerWorkflow(**self._workflow_kwargs)
        workflow.add_steps(workflow.create_steps_from_sequential_tasks([task]))
        seeds = workflow.generate_preview(verbose_logging=verbose_logging).output
        self._validate_seeds(seeds)
        return CategoricalDataSeeds(**seeds)

    def generate_dataset_preview(
        self,
        *,
        data_seeds: Optional[CategoricalDataSeeds] = None,
        verbose_logging: bool = False,
        **kwargs,
    ) -> PreviewResults:
        """Generate a preview synthetic dataset using the current workflow steps.

        Args:
            data_seeds: Data seeds to use in place of what was defined in the configuration.
                This is useful if you have pre-generated data seeds or want to experiment with
                different seed categories/values.
            verbose_logging: If True, additional logging will be displayed during execution.

        Returns:
            A PreviewResults instance containing the outputs of the workflow.
        """
        workflow = DataDesignerWorkflow(**self._workflow_kwargs)

        steps, data_seeds = self._create_workflow_steps(
            data_seeds=data_seeds,
            verbose_logging=False,
            **kwargs,
        )

        workflow.add_steps(steps)

        logger.info("🚀 Generating dataset preview")
        preview = workflow.generate_preview(verbose_logging=verbose_logging)
        if preview.output is not None:
            logger.info("👀 Your dataset preview is ready for a peek!")

        # In order to have a data spec, we need to know the final data seeds. We grab them
        # from the Workflow outputs to be certain they are the same as the ones used in the preview.
        seed_steps = [
            s.name
            for s in workflow.steps
            if workflow.step_io_map[s.name]["output"] == "categorical_data_seeds"
        ]
        if len(seed_steps) == 0:
            raise ValueError(
                "The workflow does not contain a step that outputs categorical data seeds. "
                "This is required to generate a preview dataset by upsampling sample records."
            )
        seed_step = seed_steps[-1]
        data_seeds = CategoricalDataSeeds(**preview.outputs_by_step[seed_step])

        # The data spec has info about all the various columns in the dataset.
        # We use this to display the preview data in a more human-readable format.
        preview.data_spec = self.get_data_spec(data_seeds)

        if preview.evaluation_results is not None and self._eval_type in list(
            LLMJudgePromptTemplateType
        ):
            display_preview_evaluation_summary(
                self._eval_type, preview.evaluation_results
            )
        return preview

    def submit_batch_workflow(
        self,
        *,
        num_records: int,
        project_name: Optional[str] = None,
        data_seeds: Optional[CategoricalDataSeeds] = None,
        **kwargs,
    ) -> DataDesignerBatchJob:
        """Submit a batch job to generate a synthetic dataset.

        Args:
            num_records: Number of records to generate.
            project_name: Gretel project name to use for the batch job. If None,
                a randomized project name will be generated.
            data_seeds: Data seeds to use in place of what was defined in the configuration.
                This is useful if you have pre-generated data seeds or want to experiment with
                different seed categories/values. If None, the data seeds defined in the
                configuration will be used, including generating new values if needed.

        Returns:
            NavigatorWorkflowBatchJob instance containing the workflow run details and helper
            methods for fetching the results.
        """
        workflow = DataDesignerWorkflow(**self._workflow_kwargs)
        steps, _ = self._create_workflow_steps(
            num_records=num_records,
            data_seeds=data_seeds,
            verbose_logging=True,
            **kwargs,
        )
        workflow.add_steps(steps)

        # try and reuse existing project and workflows if they have
        # already been created. in the case that these are None, they
        # will get created on first run.
        project_id = self._project_id
        workflow_id = self._workflow_id

        if project_name is not None:
            # the following block handles the case where a new project_name
            # is passed to the same session. in this case, we assume that
            # we'll need to create a new Workflow for the new project.
            if project_name != project_id:
                workflow_id = None
                project_id = project_name

        batch_job = workflow.submit_batch_job(
            num_records=num_records,
            project_name=project_id,
            workflow_id=workflow_id,
        )

        # cache the project and workflow ids so we can reuse it
        # on subsequent calls within the session.
        self._workflow_id = batch_job.workflow_id
        self._project_id = batch_job.project_id

        return batch_job

    def _validate_seeds(self, seeds: dict) -> None:
        if seeds is None or "seed_categories" not in seeds:
            raise ValueError(
                "An error occurred while generating your categorical seed values. "
                "Please check that your configuration is as expected, restart your "
                "session, and try again. If the problem persists, please contact support "
                "and/or submit a GitHub issue to the gretel-client repo."
            )

    def _create_workflow_steps(
        self,
        num_records: Optional[int] = None,
        data_seeds: Optional[CategoricalDataSeeds] = None,
        dataset_context: Optional[str] = None,
        verbose_logging: bool = False,
        **kwargs,
    ) -> tuple[list[Step], Optional[CategoricalDataSeeds]]:
        """Create workflow steps from a list of tasks.

        Args:
            num_records: Number of records to be generated.
            data_seeds: Data seeds to use in place of what was defined in the configuration.
                This is useful if you have pre-generated data seeds or want to experiment with
                different seed categories/values.
            dataset_context: Context for the dataset to be used in the seed value generation task.
            verbose_logging: If True, additional logging will be displayed.

        Returns:
            A tuple that contains a list of workflow steps and the data seeds used in the workflow.
        """
        if len(self._seed_categories) == 0:
            raise ValueError("No seed columns have been defined.")

        task_list = []
        num_records = num_records or PREVIEW_NUM_RECORDS
        data_seeds = data_seeds or self.categorical_seed_columns

        if data_seeds.needs_generation:
            # If any seed category / subcategory values need generation,
            # we start with the seed value generation task.
            task_list.append(
                GenerateSeedCategoryValues(
                    seed_categories=list(self._seed_categories.values()),
                    dataset_context=dataset_context,
                    client=self._client,
                )
            )
        else:
            # If no seed category / subcategory values need generation, we start
            # with a task that directly loads them into the workflow.
            task_list.append(
                LoadDataSeeds(
                    categorical_data_seeds=data_seeds,
                    client=self._client,
                )
            )

        # Given fully-specified data seeds, we next add a task to
        # sample them in to a seed dataset.
        task_list.append(SampleDataSeeds(num_records=num_records, client=self._client))

        # Iterate over the data columns and create generation tasks for each.
        for column in self._generated_data_columns.values():
            task = column.to_generation_task(
                self.special_system_instructions, client=self._client
            )
            task_list.append(task)

        # Add data validators to the workflow.
        for validator in self._validators.values():
            task_list.append(validator)

        # Finally, add evaluation tasks to the workflow.
        for eval_task in self._evaluators.values():
            task_list.append(eval_task)

        steps = DataDesignerWorkflow.create_steps_from_sequential_tasks(
            task_list, verbose_logging=verbose_logging
        )
        return steps, data_seeds

    def _get_data_seeds_task(
        self, dataset_context: Optional[str] = None, **kwargs
    ) -> Task:
        """Get the task that generates seed category values."""

        if len(self._seed_categories) == 0:
            raise ValueError("No seed categories have been defined.")

        if not self.categorical_seed_columns.needs_generation:
            logger.warning("⚠️ Your categorical data seeds do not require generation.")
            return LoadDataSeeds(
                categorical_data_seeds=self.categorical_seed_columns,
                client=self._client,
            )

        return GenerateSeedCategoryValues(
            seed_categories=list(self._seed_categories.values()),
            dataset_context=dataset_context,
            client=self._client,
        )

    def _validate_generated_data_column_inputs(
        self,
        name: str,
        generation_prompt: str,
        columns_to_list_in_prompt: Optional[list[str]] = None,
    ) -> tuple[str, str, list[str]]:
        """Validate that the inputs for a generated data column.

        Args:
            name: The name of the data column.
            generation_prompt: The prompt that will be used to generate the data column.
            columns_to_list_in_prompt: List of seed and/or data columns to add as
                context for the generation prompt.

        Returns:
            A tuple containing the validated inputs for the data column.
        """

        if name in self._seed_categories:
            raise ValueError(f"Column name `{name}` already exists as a seed category.")

        # Make sure the prompt follows our generic templating rules.
        assert_valid_template(generation_prompt)

        # Keywords in templates can only reference seed columns *or* data columns that
        # have been defined *before* the column that references them.
        template_kwargs = get_prompt_template_keywords(generation_prompt)
        if not template_kwargs.issubset(self.all_column_names):
            raise ValueError(
                f"The `generation_prompt` field of `{name}` contains template keywords that "
                "are not available as columns.\n"
                f"* Template keywords found in `generation_prompt`: {template_kwargs}\n"
                f"* Available seed columns: {self.categorical_seed_column_names}\n"
                f"* Available data columns: {self.generated_data_column_names}"
            )

        if isinstance(columns_to_list_in_prompt, str):
            if columns_to_list_in_prompt == "all":
                columns_to_list_in_prompt = self.all_column_names
            elif columns_to_list_in_prompt == "all_categorical_seed_columns":
                columns_to_list_in_prompt = self.categorical_seed_column_names
            elif columns_to_list_in_prompt == "all_generated_data_columns":
                if len(self.generated_data_column_names) == 0:
                    logger.warning(
                        f"⚠️ The generated data column `{name}` has set `columns_to_list_in_prompt` "
                        "to 'all_generated_data_columns', but no data columns have been defined."
                    )
                columns_to_list_in_prompt = self.generated_data_column_names
            else:
                raise ValueError(
                    f"If not None, `columns_to_list_in_prompt` must be a list of column names or "
                    "one of ['all', 'all_categorical_seed_columns', 'all_generated_data_columns']. "
                    f"You provided: {columns_to_list_in_prompt}"
                )
        else:
            columns_to_list_in_prompt = columns_to_list_in_prompt or []
            # Context can only reference columns that have been
            # defined *before* the current column.
            if any(
                col not in self.all_column_names for col in columns_to_list_in_prompt
            ):
                raise ValueError(
                    f"The `columns_to_list_in_prompt` field of `{name}` contains invalid columns. "
                    "Only seed or data columns that have been defined before the current "
                    "column can be added as context.\n"
                    f"* Available seed columns: {self.categorical_seed_column_names}\n"
                    f"* Available data columns: {self.generated_data_column_names}\n"
                )

        return name, generation_prompt, columns_to_list_in_prompt

    def __repr__(self):
        seed_categories = [
            (
                name
                if len(s.subcategories) == 0
                else f"{name}:{','.join([n.name for n in s.subcategories])}"
            )
            for name, s in self._seed_categories.items()
        ]

        categorical_seed_columns = (
            f"    categorical_seed_columns: {seed_categories}\n"
            if self.has_seed_categories
            else ""
        )

        generated_data_columns = (
            f"    generated_data_columns: {self.generated_data_column_names}\n"
            if len(self.generated_data_column_names) > 0
            else ""
        )

        validators = (
            f"    validator: {[f'{k}:{v.config.code_lang}' for k,v in self._validators.items()][0]}\n"
            if len(self._validators) > 0
            else ""
        )

        evaluator = (
            f"    evaluator: {self._eval_type}\n" if self._eval_type is not None else ""
        )

        column_repr = (
            f"{categorical_seed_columns}"
            f"{generated_data_columns}"
            f"{validators}"
            f"{evaluator}"
        )
        newline = "\n" if len(column_repr) > 0 else ""

        return f"{self.__class__.__name__}({newline}{column_repr})"
