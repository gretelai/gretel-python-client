import logging
import sys
import uuid

from pathlib import Path
from typing import Optional, Union

from gretel_client.config import configure_session
from gretel_client.dataframe import _DataFrameT
from gretel_client.evaluation.quality_report import QualityReport
from gretel_client.evaluation.reports import (
    DEFAULT_CORRELATION_COLUMNS,
    DEFAULT_RECORD_COUNT,
    DEFAULT_SQS_REPORT_COLUMNS,
)
from gretel_client.gretel.artifact_fetching import (
    fetch_final_model_config,
    fetch_model_logs,
    fetch_model_report,
    fetch_synthetic_data,
    GretelReport,
    PANDAS_IS_INSTALLED,
)
from gretel_client.gretel.config_setup import (
    CONFIG_SETUP_DICT,
    create_model_config_from_base,
)
from gretel_client.gretel.exceptions import (
    GretelJobSubmissionError,
    GretelProjectNotSetError,
)
from gretel_client.gretel.job_results import GenerateJobResults, TrainJobResults
from gretel_client.helpers import poll
from gretel_client.projects import get_project, Project
from gretel_client.projects.models import Model
from gretel_client.rest.exceptions import ApiException
from gretel_client.users.users import get_me

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


def _convert_to_valid_data_source(
    data: Union[str, Path, _DataFrameT]
) -> Union[str, _DataFrameT]:
    """Returns a valid data source (str of DataFrame) for a Gretel job."""
    return str(data) if isinstance(data, Path) else data


class Gretel:
    """High-level interface for interacting with Gretel's APIs.

    An instance of this class is bound to a single Gretel project. If a project
    name is not provided at instantiation, a new project will be created with the
    first job submission. You can change projects using the `set_project` method.

    Args:
        project_name (str): Name of new or existing project. If a new project name
            is given, it will be created at instantiation. If no name given, a new
            randomly-named project will be created with the first job submission.
        project_display_name (str): Project display name. If `None`, will use the
            project name. This argument is only used when creating a new project.
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
        *,
        project_name: Optional[str] = None,
        project_display_name: Optional[str] = None,
        **session_kwargs,
    ):
        configure_session(**session_kwargs)

        self._project: Optional[Project] = None
        self._user_id: str = get_me()["_id"][9:]

        if project_name is not None:
            self.set_project(name=project_name, display_name=project_display_name)

    def _assert_project_is_set(self):
        """Raise an error if a project has not been set."""
        if self._project is None:
            raise GretelProjectNotSetError(
                "A project must be set to fetch models and their artifacts. "
                "Use `set_project` to create or select an existing project."
            )

    def get_project(self) -> Project:
        """Returns the current Gretel project.

        If a project has not been set, a new one will be created.
        """
        if self._project is None:
            logger.info("No project set. Creating a new one...")
            self.set_project()
        return self._project

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
        name = name or f"gretel-sdk-{uuid.uuid4().hex[:5]}-{self._user_id}"

        try:
            project = get_project(
                name=name, display_name=display_name or name, desc=desc, create=True
            )
        except ApiException as exception:
            if "Project name not available" not in exception.body:
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
            )

        self._last_model = None
        self._project = project

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

    def submit_evaluate(
        self,
        real_data: Union[str, Path, _DataFrameT],
        synthetic_data: Union[str, Path, _DataFrameT],
        num_eval_records: int = DEFAULT_RECORD_COUNT,
        job_label: Optional[str] = None,
        **kwargs,
    ) -> GretelReport:
        """Submit a synthetic data quality evaluate job.

        Args:
            real_data: Real data source to compare against.
            synthetic_data: Synthetic data source to evaluate.
            num_eval_records: Number of records to sample for evaluation.
            job_label: Descriptive label to append to job the name.

        Returns:
            GretelReport object with the report as a dict and as HTML.
        """

        project = self.get_project()

        qr = QualityReport(
            project=project,
            name="evaluate" + (f"-{job_label}" if job_label else ""),
            ref_data=_convert_to_valid_data_source(real_data),
            data_source=_convert_to_valid_data_source(synthetic_data),
            record_count=num_eval_records,
            correlation_column_count=kwargs.get(
                "correlation_column_count", DEFAULT_CORRELATION_COLUMNS
            ),
            column_count=kwargs.get("column_count", DEFAULT_SQS_REPORT_COLUMNS),
            mandatory_columns=kwargs.get("mandatory_columns", []),
        )

        logger.info(
            "Submitting synthetic data evaluation job...\n"
            f"Console URL: {project.get_console_url()}/models/"
        )
        qr.run()

        return GretelReport(as_dict=qr.as_dict, as_html=qr.as_html)

    def submit_train(
        self,
        base_config: str,
        *,
        data_source: Union[str, Path, _DataFrameT],
        job_label: Optional[str] = None,
        wait: bool = True,
        **non_default_config_settings,
    ) -> TrainJobResults:
        """Submit a Gretel model training job.

        Training jobs are configured by updating a base config, which can be
        given as a yaml file path or as the name of one of the Gretel base
        config files (without the extension) listed here:
        https://github.com/gretelai/gretel-blueprints/tree/main/config_templates/gretel/synthetics

        Args:
            base_config: Gretel base config name or yaml config file path.
            data_source: Training data source as a file path or pandas DataFrame.
            job_label: Descriptive label to append to job the name.
            wait: If True, wait for the job to complete before returning.
            **non_default_config_settings: Config settings to override in the
                template. The format is `section={"setting": "value"}`,
                where `section` is the name of a yaml section within the
                specific model settings, e.g. `params` or `privacy_filters`.

        Returns:
            Job results including the model object, report, logs, and final config.

        Example::

            from gretel_client import Gretel
            gretel = Gretel(project_name="my-project")
            trained = gretel.submit_train(
                base_config="tabular-actgan",
                data_source="data.csv",
                params={"epochs": 100, "generator_dim": [128, 128]},
                privacy_filters={"similarity": "high", "outliers": None},
            )
        """
        model_config = create_model_config_from_base(
            base_config=base_config,
            job_label=job_label,
            **non_default_config_settings,
        )

        project = self.get_project()

        data_source = _convert_to_valid_data_source(data_source)
        model = project.create_model_obj(model_config, data_source=data_source)

        dict_name = list(model_config["models"][0].keys())[0]
        model_name = CONFIG_SETUP_DICT[dict_name].model_name.replace("_", "-")
        docs_path = f"reference/synthetics/models/gretel-{model_name}"
        project_url = project.get_console_url()

        logger.info(
            f"Submitting {model_name.upper()} training job...\n"
            f"Model Docs: https://docs.gretel.ai/{docs_path}"
        )

        model.submit()

        logger.info(f"Console URL: {project_url}/models/{model.model_id}/activity")

        report = None
        logs = None
        final_config = None

        if wait:
            poll(model, verbose=False)
            report = fetch_model_report(model)
            logs = fetch_model_logs(model)
            final_config = fetch_final_model_config(model)

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
            df_seed = pd.DataFrame(["rare_class"] * 1000, columns=["rare"])
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

        dict_name = list(model.model_config["models"][0].keys())[0]
        model_name = CONFIG_SETUP_DICT[dict_name].model_name.replace("_", "-")
        docs_path = f"reference/synthetics/models/gretel-{model_name}"
        project_url = project.get_console_url()

        logger.info(
            f"Submitting {model_name.upper()} generate job...\n"
            f"Model Docs: https://docs.gretel.ai/{docs_path}\n"
            f"Console URL: {project_url}/models/{model.model_id}/data"
        )

        record_handler.submit()

        synthetic_data = None
        synthetic_data_link = None

        if wait:
            poll(record_handler, verbose=False)
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

        return GenerateJobResults(
            model=model,
            project=project,
            record_handler=record_handler,
            synthetic_data=synthetic_data,
            synthetic_data_link=synthetic_data_link,
        )

    def __repr__(self):
        name = self._project.name if self._project else None
        return f"{self.__class__.__name__}(project_name={name})"
