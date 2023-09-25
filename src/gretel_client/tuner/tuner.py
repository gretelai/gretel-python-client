from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import optuna
import pandas as pd

from gretel_client.helpers import poll
from gretel_client.projects import Project, tmp_project
from gretel_client.projects.models import Model
from gretel_client.tuner.metrics import BaseTunerMetric, GretelSQSMetric
from gretel_client.tuner.sampler import BaseConfigSampler

__all__ = ["GretelHyperParameterTuner"]


@dataclass(frozen=True)
class GretelHyperParameterResults:
    best_config: dict
    optuna_study: optuna.Study
    df_trials: pd.DataFrame


class GretelHyperParameterTuner:
    """Class for running hyperparameter tuning experiments with Optuna.

    Args:
        config_sampler: Sampler for generating trial model configs.
        metric: Metric to optimize. Note that the metric is maximized.
                Defaults to GretelSQSMetric.
    """

    def __init__(
        self,
        config_sampler: BaseConfigSampler,
        metric: BaseTunerMetric = GretelSQSMetric(),
    ):
        self.sampler = config_sampler
        self.metric = metric

    def _add_gretel_metrics_to_trial(self, model: Model, trial: optuna.Trial) -> dict:
        gretel_metric_names = [
            "synthetic_data_quality_score",
            "field_correlation_stability",
            "principal_component_stability",
            "field_distribution_stability",
            "privacy_protection_level",
        ]
        gretel_report = self.metric._get_gretel_report(model)
        for n in gretel_metric_names:
            key = "grade" if n == "privacy_protection_level" else "raw_score"
            trial.set_user_attr(n, gretel_report[n][key])

    def _objective(self, trial: optuna.Trial):
        trial_config = self.sampler._get_trial_config(trial)

        try:
            trial_config["name"] = f"{trial_config['name']}-optuna-{trial.number}"
            model = self.project.create_model_obj(
                trial_config, data_source=self.data_source
            )
            model.submit()
            poll(model, verbose=False)
            self._add_gretel_metrics_to_trial(model, trial)
            return self.metric(model)

        except Exception as e:
            raise Exception(
                f"Model run failed with error {e}, using config {trial_config}"
            )

    def run(
        self,
        data_source: Union[str, Path, pd.DataFrame],
        n_trials: int = 5,
        n_jobs: int = 1,
        project: Optional[Project] = None,
        study: Optional[optuna.Study] = None,
        **kwargs,
    ) -> GretelHyperParameterResults:
        """Run Optuna hyperparameter tuning experiment.

        The experiment will run a number of trials, each with a different set of
        hyperparameters. To run jobs in parallel, set `n_jobs` > 1. The `n_jobs`
        parameter is passed directly to Optuna's `optimize` method. You ideally
        want `n_jobs` to be less than `n_trials`, since the trail results are
        used to determine the next set of hyperparameters to sample. Finally,
        note that each job will spin up a Gretel worker.

        Args:
            data_source: Training data for the synthetics model.
            n_trials: Number of Optuna parameter trials. Defaults to 5.
            n_jobs: Number of parallel jobs to submit at a time. Defaults to 1.
            project: Gretel project. If None, temp project used. Defaults to None.
            study: Optuna study. If None, new study created. Defaults to None.

        Returns:
            Results dataclass with best_config, optuna_study, and df_trials
            as attributes.
        """

        self.data_source = (
            data_source
            if not isinstance(data_source, (str, Path))
            else str(data_source)
        )

        with (tmp_project() if project is None else nullcontext(project)) as project:
            self.project = project
            self.runner_mode = project.client_config.default_runner
            self.artifact_endpoint = project.client_config.artifact_endpoint
            if study is None:
                study = optuna.create_study(
                    study_name=f"optuna-study_{self.project.name}",
                    direction="maximize",
                )
            n_jobs = n_jobs if n_jobs < n_trials else n_trials
            study.optimize(self._objective, n_trials=n_trials, n_jobs=n_jobs, **kwargs)

        return GretelHyperParameterResults(
            best_config=self.sampler.create_config(**study.best_params),
            optuna_study=study,
            df_trials=study.trials_dataframe(),
        )
