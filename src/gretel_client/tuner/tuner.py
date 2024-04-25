import logging
import sys

from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Protocol, Union

import optuna
import pandas as pd

from gretel_client.config import add_session_context, ClientConfig
from gretel_client.gretel.config_setup import (
    CONFIG_SETUP_DICT,
    get_model_docs_url,
    smart_load_yaml,
)
from gretel_client.helpers import poll
from gretel_client.projects import Project, tmp_project
from gretel_client.projects.jobs import Job
from gretel_client.projects.models import Model
from gretel_client.tuner.config_sampler import ModelConfigSampler
from gretel_client.tuner.exceptions import ModelMetricMismatchError, TunerTrialError
from gretel_client.tuner.metrics import (
    BaseTunerMetric,
    GretelMetricName,
    GretelQualityScore,
)

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG)

TUNER_SESSION_METADATA = {"tuner": "1"}


class Poll(Protocol):
    def __call__(self, job: Job, verbose: bool) -> None: ...


@dataclass(frozen=True)
class GretelTunerResults:
    best_config: dict
    best_trial_number: int
    study: optuna.Study
    trial_data: pd.DataFrame
    best_model_id: Optional[str] = None

    @property
    def n_trials(self) -> int:
        return len(self.trial_data)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(\n"
            f"    n_trials: {self.n_trials}\n"
            f"    best_trial_number: {self.best_trial_number}\n"
            f"    best_model_id: {self.best_model_id}\n"
            ")"
        )


class GretelTuner:
    """Class for running hyperparameter tuning experiments.

    Args:
        config_sampler: Sampler for generating trial model configs.
        metric: Metric to optimize. Defaults to GretelQualityScore.
            To create a custom metric, subclass `BaseTunerMetric`
            and implement a __call__ method that takes a Gretel `Model`
            as input and returns the metric score as a float.
    """

    def __init__(
        self,
        config_sampler: ModelConfigSampler,
        metric: Optional[BaseTunerMetric] = None,
    ):
        self.sampler = config_sampler
        self.metric = metric or GretelQualityScore()
        self.model_setup = CONFIG_SETUP_DICT[self.sampler.model_type]
        self.poll = poll

        if isinstance(self.metric, GretelQualityScore):
            if self.sampler.model_type not in self.metric.metric_name.compatible_models:
                raise ModelMetricMismatchError(
                    f"The '{self.metric.metric_name.value}' metric is not "
                    f"compatible with the {self.sampler.model_type.upper()} model."
                )

    def _add_gretel_metrics_to_trial(self, model: Model, trial: optuna.Trial):
        """Add Gretel's quality metrics to the Trial object as user attributes."""
        gretel_report = self.metric.get_gretel_report(model)
        scores = {d["field"]: d["value"] for d in gretel_report["summary"]}
        for k, v in scores.items():
            trial.set_user_attr(k, v)

    def _rename_trial_data_columns(self, trial_data: pd.DataFrame) -> pd.DataFrame:
        """Rename columns in the trial data to be more readable."""
        for old_prefix, new_prefix in zip(["params_", "user_attrs_"], ["", "gretel_"]):
            trial_data = trial_data.rename(
                columns={
                    c: c.replace(old_prefix, new_prefix, 1)
                    for c in [c for c in trial_data.columns if old_prefix in c]
                }
            )
        return trial_data

    def _set_poll(self, poll: Poll) -> None:
        """Replace the poll function used to wait for job completion"""
        self.poll = poll

    def _objective(self, trial: optuna.Trial) -> float:
        """Objective function for Optuna hyperparameter tuning."""
        trial_config = self.sampler.create_trial_config(trial)

        try:
            trial_config["name"] = f"{trial_config['name']}-tuner-{trial.number}"
            model = self.project.create_model_obj(
                trial_config, data_source=self._artifact_path
            )

            logger.info(
                f"Submitting {self.model_setup.model_name.upper()} job "
                f"for tuner trial {trial.number}..."
            )

            model.submit()
            self.poll(model, verbose=self._verbose_logging)

            if model.errors is not None:
                raise Exception(f"Model failed with error: {model.errors}")

            if (
                isinstance(self.metric, BaseTunerMetric)
                and self.model_setup.report_type is not None
            ):
                self._add_gretel_metrics_to_trial(model, trial)

            trial.set_user_attr("model_id", model.model_id)

            score = self.metric(model)
            sampled_params = self.sampler.parse_trial_params(trial.params)

            logger.info(f"Trial {trial.number} -> {self.metric} value: {score:.5f} ")
            logger.debug(
                f"Trial {trial.number} -> sampled parameters: {sampled_params}"
            )

            return score

        except Exception as e:
            raise TunerTrialError(
                f"Trial {trial.number} failed with error: {e}.\n"
                f"Trial config: {trial_config}"
            )

    def run(
        self,
        data_source: Union[str, Path, pd.DataFrame],
        n_trials: int = 5,
        n_jobs: int = 1,
        project: Optional[Project] = None,
        study: Optional[optuna.Study] = None,
        verbose_logging: bool = False,
        session: Optional[ClientConfig] = None,
        **kwargs,
    ) -> GretelTunerResults:
        """Run hyperparameter tuning experiment.

        The experiment will run `n_trials` trials, each with a different set of
        hyperparameters. To run jobs in parallel, set `n_jobs` > 1. You ideally
        want `n_jobs` to be less than `n_trials`, since the trail results are
        used to determine the next set of hyperparameters to sample. Note that
        each job will spin up a Gretel worker.

        Args:
            data_source: Training data for the synthetics model.
            n_trials: Number of hyperparameter trials. Defaults to 5.
            n_jobs: Number of parallel jobs to submit at a time. Defaults to 1.
            project: Gretel project. If None, a temp project used. Defaults to None.
            study: Optuna study. If None, new study is created. Defaults to None.
            verbose_logging: If True, print all logs from the submitted Gretel jobs.
            session: The client configuration to use. Takes precedence over project
                session (if provided).

        Returns:
            Results dataclass with the best config, study object, and
            trial data as attributes.
        """

        # Create tagged session based on highest-priority provided session (if any)
        if session is None and project is not None:
            session = project.session
        session = add_session_context(
            session=session, client_metrics=TUNER_SESSION_METADATA
        )

        # Update the project with the tagged session
        if project is not None:
            project = project.with_session(session)

        self.data_source = (
            str(data_source) if isinstance(data_source, Path) else data_source
        )

        is_temp_project = project is None
        optuna_user_logging_level = optuna.logging.get_verbosity()
        optuna.logging.set_verbosity(optuna.logging.WARNING)

        with (
            tmp_project(session=session) if is_temp_project else nullcontext(project)
        ) as project:
            self.project = project
            self._artifact_path = self.project.upload_artifact(self.data_source)
            self._verbose_logging = verbose_logging

            if study is None:
                study = optuna.create_study(
                    study_name=f"tuner-study_{self.project.name}",
                    direction=self.metric.direction.value,
                )

            logger.debug(
                f"Starting {self.model_setup.model_name.upper()} "
                "hyperparameter-tuning experiment...\n"
                f"Model Docs: {get_model_docs_url(self.sampler.model_type)}\n"
                f"{'Temporary ' if is_temp_project else ''}"
                f"Project URL: {self.project.get_console_url()}\n"
                f"Number of parallel jobs: {n_jobs}\n"
                f"Number of tuner trials: {n_trials}\n"
                f"Optimization metric: {self.metric}\n"
                f"Optimization direction: {self.metric.direction.value}"
            )

            study.optimize(
                self._objective,
                n_trials=n_trials,
                n_jobs=n_jobs if n_jobs < n_trials else n_trials,
                show_progress_bar=kwargs.pop("show_progress_bar", True),
                **kwargs,
            )

        optuna.logging.set_verbosity(optuna_user_logging_level)

        trial_data = self._rename_trial_data_columns(study.trials_dataframe())
        model_id = None if is_temp_project else study.best_trial.user_attrs["model_id"]

        return GretelTunerResults(
            best_config=self.sampler.convert_trial_params_to_config(study.best_params),
            study=study,
            best_trial_number=study.best_trial.number,
            trial_data=trial_data,
            best_model_id=model_id,
        )


def create_tuner_from_config(
    config: Union[str, Path, dict], **non_default_config_settings
) -> GretelTuner:
    """Create a GretelTuner object from the given config.

    Args:
        config: The config as a yaml path, yaml string, or dict.

    Returns:
        The GretelTuner object.
    """
    tuner_config = smart_load_yaml(config)

    # Remove "metric" from the kwargs before passing to the sampler,
    # which would try to treat it as a config setting.
    metric = non_default_config_settings.pop("metric", None)

    sampler = ModelConfigSampler(tuner_config, **non_default_config_settings)

    if not isinstance(metric, BaseTunerMetric):
        metric = GretelQualityScore(
            GretelMetricName(metric)
            if metric is not None
            else GretelMetricName(tuner_config.get("metric", GretelMetricName.SQS))
        )

    return GretelTuner(config_sampler=sampler, metric=metric)
