from __future__ import annotations

import logging
import sys
import uuid

from pathlib import Path
from typing import Optional, TYPE_CHECKING, Union

from gretel_client.config import (
    add_session_context,
    ClientConfig,
    configure_session,
    get_session_config,
)
from gretel_client.dataframe import _DataFrameT
from gretel_client.factories import GretelFactories
from gretel_client.gretel.artifact_fetching import (
    fetch_final_model_config,
    fetch_model_logs,
    fetch_model_report,
    fetch_synthetic_data,
    PANDAS_IS_INSTALLED,
)
from gretel_client.gretel.config_setup import (
    CONFIG_SETUP_DICT,
    create_model_config_from_base,
    extract_model_config_section,
    get_model_docs_url,
    smart_read_model_config,
)
from gretel_client.gretel.exceptions import (
    GretelJobSubmissionError,
    GretelProjectNotSetError,
)
from gretel_client.gretel.job_results import (
    EvaluateResults,
    GenerateJobResults,
    TrainJobResults,
    TransformResults,
)
from gretel_client.helpers import poll
from gretel_client.inference_api.third_party.aws import BedrockAdapter, SagemakerAdapter
from gretel_client.inference_api.third_party.azure_openai import AzureOpenAIAdapter
from gretel_client.projects import get_project, Project
from gretel_client.projects.jobs import Status
from gretel_client.projects.models import Model
from gretel_client.rest.exceptions import ApiException, NotFoundException
from gretel_client.users.users import get_me

try:
    from gretel_client.tuner.tuner import create_tuner_from_config

    HAS_TUNER = True
except ImportError:
    HAS_TUNER = False

if TYPE_CHECKING:
    from botocore.client import BaseClient
    from mypy_boto3_sagemaker_runtime import SageMakerRuntimeClient
    from openai import AzureOpenAI


logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

HIGH_LEVEL_SESSION_METADATA = {"high_level_interface": "1"}
TRANSFORM_DOCS_URL = "https://docs.gretel.ai/create-synthetic-data/models/transform/v2"
EVALUATE_DOCS_URL = "https://docs.gretel.ai/optimize-synthetic-data/evaluate"


def _convert_to_valid_data_source(
    data: Optional[Union[str, Path, _DataFrameT]] = None
) -> Optional[Union[str, _DataFrameT]]:
    """Returns a valid data source (str, DataFrame, or None) for a Gretel job."""
    return str(data) if isinstance(data, Path) else data


class Gretel:
    """High-level interface for interacting with Gretel's APIs.

    To bound an instance of this class to a Gretel project, provide a project
    name at instantiation or use the `set_project` method. If a job is submitted
    (via a `submit_*` method) without a project set, a randomly-named project will
    be created and set as the current project.

    Args:
        project_name (str): Name of new or existing project. If a new project name
            is given, it will be created at instantiation. If no name given, a new
            randomly-named project will be created with the first job submission.
        project_display_name (str): Project display name. If `None`, will use the
            project name. This argument is only used when creating a new project.
        session (ClientConfig): Client session to use. If set, no ``session_kwargs``
            may be specified.
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

    _session: ClientConfig

    def __init__(
        self,
        *,
        project_name: Optional[str] = None,
        project_display_name: Optional[str] = None,
        session: Optional[ClientConfig] = None,
        skip_configure_session: Optional[bool] = False,
        **session_kwargs,
    ):
        if session is None:
            # Only used for unit tests
            if not skip_configure_session:
                configure_session(**session_kwargs)
            session = get_session_config()
        elif len(session_kwargs) > 0:
            raise ValueError("cannot specify session arguments when passing a session")

        self._session = add_session_context(
            session=session, client_metrics=HIGH_LEVEL_SESSION_METADATA
        )
        self._user_id: str = get_me(session=self._session)["_id"][9:]
        self._project: Optional[Project] = None
        self.factories = GretelFactories(
            session=self._session, skip_configure_session=skip_configure_session
        )

        if project_name is not None:
            self.set_project(name=project_name, display_name=project_display_name)

    def _assert_project_is_set(self):
        """Raise an error if a project has not been set."""
        if self._project is None:
            raise GretelProjectNotSetError(
                "A project must be set to run this method. "
                "Use `set_project` to create or select an existing project."
            )

    def _generate_random_label(self) -> str:
        return f"{uuid.uuid4().hex[:5]}-{self._user_id}"

    def get_project(self, **kwargs) -> Project:
        """Returns the current Gretel project.

        If a project has not been set, a new one will be created. The optional
        kwargs are the same as those available for the `set_project` method.
        """
        if self._project is None:
            logger.info("No project set -> creating a new one...")
            self.set_project(**kwargs)
        return self._project  # type: ignore

    def set_project(
        self,
        name: Optional[str] = None,
        desc: Optional[str] = None,
        display_name: Optional[str] = None,
    ):
        """Set the current Gretel project.

        If a project with the given name does not exist, it will be created. If
        the name is not unique, the user id will be appended to the name.

        Args:
            name: Name of new or existing project. If None, will create one.
            desc: Project description.
            display_name: Project display name. If None, will use project name.

        Raises:
            ApiException: If an error occurs while creating the project.
        """
        name = name or f"gretel-sdk-{self._generate_random_label()}"

        try:
            project = get_project(
                name=name,
                display_name=display_name or name,
                desc=desc,
                create=True,
                session=self._session,
            )
        except (ApiException, NotFoundException) as exception:
            if (
                "Project name not available" not in exception.body  # type: ignore
                and "not found" not in exception.body  # type: ignore
            ):
                raise exception
            logger.warning(
                f"Project name `{name}` is not unique -> "
                "appending your user id to the name."
            )
            unique_name = f"{name}-{self._user_id}"
            project = get_project(
                name=unique_name,
                display_name=display_name or name,
                desc=desc,
                create=True,
                session=self._session,
            )

        self._last_model = None
        self._project = project
        logger.info(f"Project URL: {project.get_console_url()}")

    def fetch_model(self, model_id: str) -> Model:
        """Fetch a Gretel model using its ID.

        You must set a project before calling this method.

        Args:
            model_id: The Gretel model ID.

        Raises:
            GretelProjectNotSetError: If a project has not been set.

        Returns:
            The Gretel model object.
        """
        self._assert_project_is_set()
        if self._last_model is None or self._last_model.model_id != model_id:
            # Save last model to avoid unnecessary API calls.
            self._last_model = Model(project=self.get_project(), model_id=model_id)
        return self._last_model

    def fetch_train_job_results(self, model_id: str) -> TrainJobResults:
        """Fetch the results object from a Gretel training job.

        You must set a project before calling this method.

        Args:
            model_id: The Gretel model ID.

        Raises:
            GretelProjectNotSetError: If a project has not been set.

        Returns:
            Job results including the model object, report, logs, and final config.
        """
        self._assert_project_is_set()
        model = self.fetch_model(model_id)
        trained = TrainJobResults(project=self.get_project(), model=model)
        trained.refresh()
        return trained

    def fetch_generate_job_results(
        self, model_id: str, record_id: str
    ) -> GenerateJobResults:
        """Fetch the results object from a Gretel generate job.

        Args:
            model_id: The Gretel model ID.
            record_id: The Gretel record handler ID.

        Raises:
            GretelProjectNotSetError: If a project has not been set.

        Returns:
            Job results including the model object, record handler, and synthetic data.
        """
        self._assert_project_is_set()
        model = self.fetch_model(model_id)
        generated = GenerateJobResults(
            model=model,
            project=self.get_project(),
            record_handler=model.get_record_handler(record_id),
        )
        generated.refresh()
        return generated

    def submit_train(
        self,
        base_config: Union[str, Path, dict],
        *,
        data_source: Union[str, Path, _DataFrameT, None],
        job_label: Optional[str] = None,
        wait: bool = True,
        verbose_logging: bool = False,
        **non_default_config_settings,
    ) -> TrainJobResults:
        """Submit a Gretel model training job.

        Training jobs are configured by updating a base config, which can be
        given as a dict, yaml file path, yaml string, or as the name of one of
        the Gretel base config files (without the extension) listed here:
        https://github.com/gretelai/gretel-blueprints/tree/main/config_templates/gretel/synthetics

        Args:
            base_config: Base config name, yaml file path, yaml string, or dict.
            data_source: Training data source as a file path or pandas DataFrame.
            job_label: Descriptive label to append to job the name.
            wait: If True, wait for the job to complete before returning.
            verbose_logging: If True, will print all logs from the job.
            **non_default_config_settings: Config settings to override in the
                template. The format is `section={"setting": "value"}`,
                where `section` is the name of a yaml section within the
                specific model settings, e.g. `params` or `privacy_filters`.
                If the parameter is not nested within a section, pass it
                directly as a keyword argument.

        Returns:
            Job results including the model object, report, logs, and final config.

        Example::

            from gretel_client import Gretel

            data_source="https://gretel-public-website.s3-us-west-2.amazonaws.com/datasets/USAdultIncome5k.csv"

            gretel = Gretel(project_name="my-project")
            trained = gretel.submit_train(
                base_config="tabular-actgan",
                data_source=data_source,
                params={"epochs": 100, "generator_dim": [128, 128]},
                privacy_filters={"similarity": "high", "outliers": None},
            )
        """
        job_config = create_model_config_from_base(
            base_config=base_config,
            job_label=job_label,
            **non_default_config_settings,
        )

        data_source = _convert_to_valid_data_source(data_source)

        model_type, model_config_section = extract_model_config_section(job_config)
        model_setup = CONFIG_SETUP_DICT[model_type]  # type: ignore
        model_name = model_setup.model_name.replace("_", " ")

        if data_source is None:
            if not model_setup.data_source_optional:
                raise GretelJobSubmissionError(
                    f"A data source must be provided for {model_name.upper()}."
                )
            model_config_section["data_source"] = None

        project = self.get_project()

        model = project.create_model_obj(job_config, data_source=data_source)
        project_url = project.get_console_url()

        logger.info(
            f"Submitting {model_name.upper()} training job...\n"
            f"Model Docs: {get_model_docs_url(model_type)}"
        )

        model.submit()

        logger.info(
            f"Console URL: {project_url}/models/{model.model_id}/activity\n"
            f"Model ID: {model.model_id}"
        )

        report = None
        logs = None
        final_config = None

        if wait:
            poll(model, verbose=verbose_logging)
            logs = fetch_model_logs(model)
            final_config = fetch_final_model_config(model)
            if (
                model_setup.report_type is not None
                and model_config_section.get("data_source") is not None
                and model.status == Status.COMPLETED
                and not model_config_section.get("evaluate", {}).get("skip", False)
            ):
                report = fetch_model_report(model, model_setup.report_type)

            if model.status != Status.COMPLETED:
                logger.warning(
                    "Training didn't complete successfully. Job status was "
                    f"'{model.status}', details: {model.errors}"
                )

        self._last_model = model

        return TrainJobResults(
            project=project,
            model=model,
            report=report,
            model_logs=logs,
            model_config=final_config,
        )

    def submit_generate(
        self,
        model_id: str,
        *,
        num_records: Optional[int] = None,
        seed_data: Optional[Union[str, Path, _DataFrameT]] = None,
        wait: bool = True,
        fetch_data: bool = True,
        verbose_logging: bool = False,
        **generate_kwargs,
    ) -> GenerateJobResults:
        """Submit a Gretel model generate job.

        Only one of `num_records` or `seed_data` can be provided. The former
        will generate a complete synthetic dataset, while the latter will
        conditionally generate synthetic data based on the seed data.

        Args:
            model_id: The Gretel model ID.
            num_records: Number of records to generate.
            seed_data: Seed data source as a file path or pandas DataFrame.
            wait: If True, wait for the job to complete before returning.
            fetch_data: If True, fetch the synthetic data as a DataFrame.
            verbose_logging: If True, will print all logs from the job.

        Raises:
            GretelJobSubmissionError: If the combination of arguments is invalid.

        Returns:
            Job results including the model object, record handler, and synthetic data.

        Examples::

            # Generate a synthetic dataset with 1000 records.
            from gretel_client import Gretel
            gretel = Gretel(project_name="my-project")
            generated = gretel.submit_generate(model_id, num_records=100)

            # Conditionally generate synthetic examples of a rare class.
            import pandas pd
            from gretel_client import Gretel
            gretel = Gretel(project_name="my-project")
            df_seed = pd.DataFrame(["rare_class"] * 1000, columns=["field_name"])
            generated = gretel.submit_generate(model_id, seed_data=df_seed)
        """

        if num_records is not None and seed_data is not None:
            raise GretelJobSubmissionError(
                "Only one of `num_records` or `seed_data` can be provided."
            )

        if num_records is None and seed_data is None:
            raise GretelJobSubmissionError(
                "Either `num_records` or `seed_data` must be provided."
            )

        if num_records is not None:
            generate_kwargs.update({"num_records": num_records})

        project = self.get_project()

        model = self.fetch_model(model_id)
        record_handler = model.create_record_handler_obj(
            data_source=_convert_to_valid_data_source(seed_data), params=generate_kwargs
        )

        model_type, _ = extract_model_config_section(model.model_config)
        model_name = CONFIG_SETUP_DICT[model_type].model_name.replace("_", " ")  # type: ignore
        project_url = project.get_console_url()

        logger.info(
            f"Submitting {model_name.upper()} generate job...\n"
            f"Model Docs: {get_model_docs_url(model_type)}\n"
            f"Console URL: {project_url}/models/{model.model_id}/data"
        )

        record_handler.submit()

        synthetic_data = None
        synthetic_data_link = None

        if wait:
            poll(record_handler, verbose=verbose_logging)
            if record_handler.status == Status.COMPLETED:
                synthetic_data_link = record_handler.get_artifact_link("data")
                if fetch_data:
                    if PANDAS_IS_INSTALLED:
                        synthetic_data = fetch_synthetic_data(record_handler)
                    else:
                        logger.warning(
                            "`fetch_data` is True, but pandas is not installed. "
                            "Only the synthetic data link will be returned. "
                            "Install pandas by running `pip install pandas`."
                        )
            else:
                logger.warning(
                    "Generation didn't complete successfully. Job status was "
                    f"'{record_handler.status}', details: {record_handler.errors}"
                )

        return GenerateJobResults(
            model=model,
            project=project,
            record_handler=record_handler,
            synthetic_data=synthetic_data,
            synthetic_data_link=synthetic_data_link,
        )

    def run_tuner(
        self,
        tuner_config: Union[str, Path, dict],
        *,
        data_source: Union[str, Path, _DataFrameT],
        n_trials: int = 5,
        n_jobs: int = 1,
        use_temporary_project: bool = False,
        verbose_logging: bool = False,
        **non_default_config_settings,
    ):
        """Run a hyperparameter tuning experiment with Gretel Tuner.

        Args:
            tuner_config: The config as a yaml file path, yaml string, or dict.
            data_source: Training data source as a file path or pandas DataFrame.
            n_trials: Number of trials to run.
            n_jobs: Number of parallel jobs to run locally. Note each job will
                spin up a Gretel worker.
            use_temporary_project: If True, will create a temporary project for
                the tuning experiment. The project will be deleted when the
                experiment is complete. If False, will use the current project.
            verbose_logging: If True, will print all logs from submitted Gretel jobs.
            **non_default_config_settings: Config settings to override in the
                given tuner config. The kwargs must follow the same nesting
                format as the yaml config file. See example below.

        Raises:
            ImportError: If the Gretel Tuner is not installed.

        Returns:
            Tuner results dataclass with the best config, best model id, study object,
            and trial data as attributes.

        Example::

            from gretel_client import Gretel
            gretel = Gretel(api_key="prompt")

            yaml_config_string = '''
            base_config: "tabular-actgan"
            metric: synthetic_data_quality_score
            params:
                epochs:
                    fixed: 50
                batch_size:
                    choices: [500, 1000]
            privacy_filters:
                similarity:
                    choices: ["medium", "high"]
            '''

            data_source="https://gretel-public-website.s3-us-west-2.amazonaws.com/datasets/USAdultIncome5k.csv"

            results = gretel.run_tuner(
                tuner_config=yaml_config_string,
                data_source=data_source,
                n_trials=2,
                params={
                    "batch_size": {"choices": [50, 100]},
                    "generator_lr": {"log_range": [0.001, 0.01]}
                },
                privacy_filters={"similarity": {"choices": [None, "medium", "high"]}},
            )

            print(f"Best config: {results.best_config}")

            # generate data with best model
            generated = gretel.submit_generate(results.best_model_id, num_records=100)
        """
        if not HAS_TUNER:
            raise ImportError(
                "This method requires the Gretel Tuner. To install, run "
                "`pip install -U gretel-client[tuner]`"
            )

        study = non_default_config_settings.pop("study", None)

        tuner = create_tuner_from_config(  # type: ignore
            config=tuner_config, **non_default_config_settings
        )

        results = tuner.run(
            data_source=_convert_to_valid_data_source(data_source),  # type: ignore
            project=self.get_project() if not use_temporary_project else None,
            n_jobs=n_jobs,
            n_trials=n_trials,
            study=study,
            verbose_logging=verbose_logging,
            session=self._session,
        )

        return results

    def submit_transform(
        self,
        config: Union[str, Path, dict],
        *,
        data_source: Union[str, Path, _DataFrameT, None],
        job_label: Optional[str] = None,
        wait: bool = True,
        verbose_logging: bool = False,
    ) -> TransformResults:
        """Transform a dataset using Gretel Transform v2.

        Args:
            config: The transform config as a yaml file path, yaml string, or dict.
            data_source: Training data source as a file path or pandas DataFrame.
            job_label: Descriptive label to append to job the name.
            wait: If True, wait for the job to complete before returning.
            verbose_logging: If True, will print all logs from the job.

        Raises:
            GretelJobSubmissionError: If any model besides transform v2 is used or an error occurs during submission
            ModelConfigReadError: If the model config is invalid

        Returns:
            Transform results dataclass with the transformed_dataframe, transform_logs
            and as attributes.

        Example::

            from gretel_client import Gretel
            gretel = Gretel(api_key="prompt")

            config = '''schema_version: "1.0"

            name: "transform v2 test model"

            models:
              - transform_v2:
                  data_source: "_"
                  globals:
                    seed: 123
                  classify:
                    enabled: false
                  steps:
                    - rows:
                        update:
                          - name: age
                            value: fake(seed=this).bothify(text="##")
            '''

            data_source="https://gretel-public-website.s3-us-west-2.amazonaws.com/datasets/USAdultIncome5k.csv"

            transform_result = gretel.submit_transform(
                config=config,
                data_source=tabular_data_source,
                job_label="testing123",
            )

            # access resulted transform file
            transformed_data = transform_result.transformed_df
        """
        job_config = smart_read_model_config(config, config_name_prefix="transform_v2")

        model_type, _ = extract_model_config_section(job_config)
        if model_type != "transform_v2":
            raise GretelJobSubmissionError(
                "Only transform_v2 is supported for 'submit_transform'"
            )

        if job_label is not None:
            job_config["name"] = f"{job_config['name']}-{job_label}"

        data_source = _convert_to_valid_data_source(data_source)
        project = self.get_project()

        model = project.create_model_obj(
            model_config=job_config, data_source=data_source
        )
        project_url = project.get_console_url()
        logger.info(
            f"Submitting transform job...\nTransform Docs: {TRANSFORM_DOCS_URL}"
        )

        model.submit()

        logger.info(
            f"Console URL: {project_url}/models/{model.model_id}/activity\n"
            f"Model ID: {model.model_id}"
        )

        transform_result = TransformResults(project=project, model=model)
        if wait:
            poll(model, verbose=verbose_logging)
            if model.status != Status.COMPLETED:
                logger.warning(
                    "Transform didn't complete successfully. Job status was "
                    f"'{model.status}', details: {model.errors}"
                )
            transform_result.refresh()

        self._last_model = model
        return transform_result

    def fetch_transform_results(self, model_id: str) -> TransformResults:
        """Fetch the results object from a Gretel transform job.

        Args:
            model_id: The Gretel model ID.

        Raises:
            GretelProjectNotSetError: If a project has not been set.

        Returns:
            Job results including the model object, report, logs, and final config.
        """
        self._assert_project_is_set()
        model = self.fetch_model(model_id)
        results = TransformResults(project=self.get_project(), model=model)
        results.refresh()
        return results

    def submit_evaluate(
        self,
        config: Union[str, Path, dict],
        *,
        data_source: Union[str, Path, _DataFrameT, None],
        ref_data: Union[str, Path, _DataFrameT, None],
        test_data: Optional[Union[str, Path, _DataFrameT, None]] = None,
        job_label: Optional[str] = None,
        wait: bool = True,
        verbose_logging: bool = False,
        **non_default_config_settings,
    ) -> EvaluateResults:
        """Create an Evaluate Model.

        Args:
            config: The evaluate config as a yaml file path, yaml string, or dict.
            data_source: Data source as a file path or pandas DataFrame to train on and produce synthetic data.
            ref_data: Reference data source as a file path or pandas DataFrame.
            test_data: Optional test data source as a file path or pandas DataFrame. This file will be used to calculate MIA, for more information about Data Privacy Metrics, please refer to our doc site, https://docs.gretel.ai/optimize-synthetic-data/evaluate/synthetic-data-quality-report.
            job_label: Descriptive label to append to job the name.
            wait: If True, wait for the job to complete before returning.
            verbose_logging: If True, will print all logs from the job.

        Raises:
            GretelJobSubmissionError: If any model besides evaluate is used or an error occurs during submission
            ModelConfigReadError: If the model config is invalid

        Returns:
            Evaluate results dataclass with evaluate_report and evaluate_logs.

        Example::

            from gretel_client import Gretel
            gretel = Gretel(api_key="prompt")

            config = '''schema_version: "1.0"

            name: "evaluate test model"

            models:
              - evaluate:
                  data_source: "_"
            '''

            synth_data_source = "https://gretel-public-website.s3-us-west-2.amazonaws.com/datasets/USAdultIncome5kGenerated.csv"
            train_ref_data = "https://gretel-public-website.s3-us-west-2.amazonaws.com/datasets/USAdultIncome4k.csv"
            test_data = "https://gretel-public-website.s3.us-west-2.amazonaws.com/datasets/USAdultIncome1k.csv"
            evaluate_result = gretel.submit_evaluate(
                config=config,
                data_source=synth_data_source,
                ref_data=train_ref_data,
                test_data=test_data,
                job_label="testing123",
            )

            # access resulted evaluate report
            evaluate_report = evaluate_result.evaluate_report
        """
        job_config = smart_read_model_config(config, config_name_prefix="evaluate")

        model_type, model_config_section = extract_model_config_section(job_config)

        if model_type != "evaluate":
            raise GretelJobSubmissionError(
                "Only `evaluate` is supported for 'submit_evaluate'"
            )

        if non_default_config_settings:
            model_config_section.update(non_default_config_settings)

        if job_label is not None:
            job_config["name"] = f"{job_config['name']}-{job_label}"

        data_source = _convert_to_valid_data_source(data_source)
        ref_data = _convert_to_valid_data_source(ref_data)
        if test_data is not None:
            test_data = _convert_to_valid_data_source(test_data)
            ref_data = [ref_data, test_data]

        project = self.get_project()

        model = project.create_model_obj(
            model_config=job_config, data_source=data_source, ref_data=ref_data
        )
        project_url = project.get_console_url()
        logger.info(f"Submitting evaluate job...\nEvaluate Docs: {EVALUATE_DOCS_URL}")

        model.submit()

        logger.info(
            f"Console URL: {project_url}/models/{model.model_id}/activity\n"
            f"Model ID: {model.model_id}"
        )

        evaluate_result = EvaluateResults(project=project, model=model)
        if wait:
            poll(model, verbose=verbose_logging)
            if model.status != Status.COMPLETED:
                logger.warning(
                    "Evaluate didn't complete successfully. Job status was "
                    f"'{model.status}', details: {model.errors}"
                )
            evaluate_result.refresh()

        self._last_model = model
        return evaluate_result

    def __repr__(self):
        name = self._project.name if self._project else None
        return f"{self.__class__.__name__}(project_name={name})"

    @staticmethod
    def create_navigator_azure_oai_adapter(
        open_ai_client: AzureOpenAI,
    ) -> AzureOpenAIAdapter:
        """
        Create an adapter for using Gretel's Navigator Tabular API with Azure OpenAI endpoints.

        Args:
            open_ai_client: An instance of AzureOpenAI from the `openai` package.
        """
        if not PANDAS_IS_INSTALLED:
            raise ImportError(
                "This adapter requires pandas. To install, run `pip install pandas`"
            )
        return AzureOpenAIAdapter(open_ai_client)

    @staticmethod
    def create_navigator_sagemaker_adapter(
        sagemaker_client: SageMakerRuntimeClient, endpoint_name: str
    ) -> SagemakerAdapter:
        """
        Create an adapter for using Gretel's Navigator Tabular API with AWS SageMaker endpoints.

        Args:
            sagemaker_client: An instance of SageMakerRuntimeClient from the `mypy_boto3_sagemaker_runtime` package.
            endpoint_name: The name of the SageMaker endpoint.
        """
        if not PANDAS_IS_INSTALLED:
            raise ImportError(
                "This adapter requires pandas. To install, run `pip install pandas`"
            )
        return SagemakerAdapter(sagemaker_client, endpoint_name)

    @staticmethod
    def create_navigator_bedrock_adapter(
        client: BaseClient, endpoint_arn: str
    ) -> BedrockAdapter:
        """
        Create an adapter for using Gretel's Navigator Tabular API with Bedrock endpoints.

        Args:
            client: An instance of the bedrock client from the `boto3` package.
            endpoint_arn: The ARN of the Bedrock (Sagemaker) endpoint.
        """
        if not PANDAS_IS_INSTALLED:
            raise ImportError(
                "This adapter requires pandas. To install, run `pip install pandas`"
            )
        return BedrockAdapter(client, endpoint_arn)
