import logging

from typing import Optional

import pandas as pd

from gretel_client.config import ClientConfig
from gretel_client.navigator.data_designer.interface import DataDesigner
from gretel_client.navigator.log import get_logger
from gretel_client.navigator.tasks.constants import S2D_PREVIEW_NUM_RECORDS
from gretel_client.navigator.tasks.extract_data_seeds_from_sample_records import (
    ExtractDataSeedsFromSampleRecords,
)
from gretel_client.navigator.tasks.generate_dataset_from_sample_records import (
    GenerateDatasetFromSampleRecords,
)
from gretel_client.navigator.tasks.load_data_seeds import LoadDataSeeds
from gretel_client.navigator.tasks.seed.sample_data_seeds import SampleDataSeeds
from gretel_client.navigator.tasks.types import (
    CategoricalDataSeeds,
    DEFAULT_MODEL_SUITE,
    LLMJudgePromptTemplateType,
    ModelSuite,
    RecordsT,
    SQL_DIALECTS,
)
from gretel_client.navigator.tasks.utils import process_sample_records
from gretel_client.navigator.tasks.validate.validate_code import (
    VALIDATE_PYTHON_COLUMN_SUFFIXES,
    VALIDATE_SQL_COLUMN_SUFFIXES,
)
from gretel_client.navigator.workflow import DataDesignerWorkflow, DataSpec, Step

logger = get_logger(__name__, level=logging.INFO)


class DataDesignerFromSampleRecords(DataDesigner):
    """DataDesigner subclass that is initialized from a sample of records.

    Use this subclass of DataDesigner when you want to turn a few sample records
    into a rich, diverse synthetic dataset (Sample-to-Dataset).

    Args:
        sample_records: Sample records from which categorical data seeds will be extracted
            and optionally used to create generated data columns.
        subsample_size: The number of records to use from the sample records. If None,
            all records will be used. If the subsample size is larger than the sample records,
            the full sample will be used.
        model_suite: The model suite to use for generating synthetic data. Defaults to the
            apache-2.0 licensed model suite.
        special_system_instructions: Optional instructions for the system to follow when generating
            the dataset. These instructions will be added to the system prompts.
        session: Optional Gretel session configuration object. If not provided, the session will be
            configured based on the provided session_kwargs or cached session configuration.
        **session_kwargs: kwargs for your Gretel session. See options below.

    Keyword Args:
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

    def __init__(
        self,
        sample_records: RecordsT,
        *,
        subsample_size: Optional[int] = None,
        model_suite: ModelSuite = DEFAULT_MODEL_SUITE,
        session: Optional[ClientConfig] = None,
        **session_kwargs,
    ):
        super().__init__(model_suite=model_suite, session=session, **session_kwargs)

        processed_sample_records = process_sample_records(
            sample_records,
            subsample_size=subsample_size,
        )

        self._sample_records: Optional[RecordsT] = processed_sample_records

        for name in list(pd.DataFrame.from_records(self._sample_records)):
            self.add_generated_data_column(
                name, generation_prompt="(from_sample_records)"
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
            num_records: The number of records to generate.
            data_seeds: Data seeds to use in place of what was defined in the configuration.
                This is useful if you have pre-generated data seeds or want to experiment with
                different seed categories/values.
            dataset_context: Context for the dataset to be used in the seed generation task.
            verbose_logging: If True, additional logging will be displayed.

        Returns:
            A tuple that contains a list of workflow steps and the data seeds used in the workflow.
        """
        if len(self._seed_categories) > 0 and data_seeds is None:
            data_seeds = self.categorical_seed_columns
            if data_seeds.needs_generation:
                raise ValueError(
                    "Category value generation is not supported for seeds that were "
                    "extracted from a data sample. Please provide pre-generated seed values "
                    "or switch to initializing DataDesigner directly or from a config."
                )

        task_list = []
        num_records = num_records or S2D_PREVIEW_NUM_RECORDS

        # We allow arguments from the extraction and data generation tasks to be passed as kwargs.
        # Pop extraction task args so that only data generation args remain.
        extract_data_seeds_kwargs = {
            k: kwargs.pop(k)
            for k in list(kwargs.keys())
            if k in ["max_num_seeds", "num_assistants", "system_prompt_type"]
        }

        if data_seeds is not None:
            task_list.append(
                LoadDataSeeds(categorical_data_seeds=data_seeds, client=self._client)
            )

        else:
            task_list.append(
                ExtractDataSeedsFromSampleRecords(
                    sample_records=self._sample_records,
                    client=self._client,
                    dataset_context=dataset_context,
                    **extract_data_seeds_kwargs,
                )
            )

        # If any generated data columns are based on sample records,
        # we are in the full sample-to-dataset use case
        if any(
            column.generation_prompt == "(from_sample_records)"
            for column in self._generated_data_columns.values()
        ):
            task_list.append(
                GenerateDatasetFromSampleRecords(
                    sample_records=self._sample_records,
                    client=self._client,
                    dataset_context=dataset_context,
                    target_num_records=num_records,
                    **kwargs,
                )
            )

        # Otherwise, we need to sample the data seeds to generate a dataset.
        else:
            task_list.append(
                SampleDataSeeds(num_records=num_records, client=self._client)
            )

        # Append all other generated data columns that have been added.
        for column in self._generated_data_columns.values():
            if column.generation_prompt == "(from_sample_records)":
                continue

            task_list.append(
                column.to_generation_task(
                    self.special_system_instructions, client=self._client
                )
            )

        for validator in self._validators.values():
            task_list.append(validator)

        for eval_task in self._evaluators.values():
            task_list.append(eval_task)

        steps = DataDesignerWorkflow.create_steps_from_sequential_tasks(
            task_list, verbose_logging=verbose_logging
        )

        return steps, data_seeds

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
        if self.has_seed_categories:
            data_seeds = super().run_data_seeds_step(
                dataset_context=dataset_context,
                verbose_logging=verbose_logging,
                kwargs=kwargs,
            )
        else:
            data_seeds = CategoricalDataSeeds(seed_categories=[])

        # Extract seeds from samples
        task = ExtractDataSeedsFromSampleRecords(
            sample_records=self._sample_records,
            client=self._client,
            dataset_context=dataset_context,
            **kwargs,
        )
        workflow = DataDesignerWorkflow(**self._workflow_kwargs)
        workflow.add_steps(workflow.create_steps_from_sequential_tasks([task]))
        seeds = workflow.generate_preview(verbose_logging=verbose_logging).output
        self._validate_seeds(seeds)
        seeds_from_samples = CategoricalDataSeeds(**seeds)
        data_seeds.add(seeds_from_samples.seed_categories)
        return data_seeds

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

        # We need this step for sample-to-dataset because we sometimes change
        # the column names to follow a standard format.
        if data_seeds is not None and data_seeds.dataset_schema_map is not None:
            generated_data_column_names = []
            schema_map = data_seeds.dataset_schema_map.get("original_to_new", {})
            for col in self.generated_data_column_names:
                if col in schema_map:
                    generated_data_column_names.append(schema_map[col])
                else:
                    generated_data_column_names.append(col)
        else:
            generated_data_column_names = self.generated_data_column_names

        return DataSpec(
            seed_category_names=seed_category_names,
            seed_subcategory_names=seed_subcategory_names,
            data_column_names=generated_data_column_names,
            validation_column_names=validation_columns,
            code_column_names=code_columns,
            code_lang=code_lang,
            eval_type=self._eval_type,
            llm_judge_column_name=llm_judge_column_name,
        )

    def __repr__(self):
        if len(self._seed_categories) > 0:
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
                if len(seed_categories) > 0
                else ""
            )
        else:
            categorical_seed_columns = (
                "    categorical_seed_columns: (from_sample_records)\n"
            )

        generated_data_columns = [
            (
                f"{c.name} (from_sample_records)"
                if c.generation_prompt == "(from_sample_records)"
                else c.name
            )
            for c in self._generated_data_columns.values()
        ]

        generated_data_columns = (
            f"    generated_data_columns: {generated_data_columns}\n"
            if len(generated_data_columns) > 0
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
