from abc import abstractmethod
from copy import deepcopy
from typing import List, Tuple

import optuna

from pydantic import BaseModel, Field

from gretel_client.projects.models import read_model_config

__all__ = ["BaseConfigSampler", "ACTGANConfigSampler"]


class BaseConfigSampler(BaseModel):
    """Base class for config samplers."""

    base_model_config: dict

    class Config:
        arbitrary_types_allowed = True
        validate_assignment = True

    @abstractmethod
    def _get_trial_config(self, trial: optuna.Trial) -> dict:
        """Return a model config dict for a single Optuna trial."""
        ...

    @abstractmethod
    def create_config(self, **kwargs) -> dict:
        """Return a model config given specific sampler parameter values."""
        ...


class ACTGANConfigSampler(BaseConfigSampler):
    base_model_config: dict = Field(
        default=read_model_config("synthetics/tabular-actgan")
    )
    epochs_choices: List[int] = Field(default=[100, 200, 400, 800], gt=0)
    batch_size_choices: List[int] = Field(default=[1000], gt=0)
    layer_width_choices: List[int] = Field(default=[512, 1024, 2048], gt=0)
    num_layers_range: Tuple[int, int] = Field(default=(2, 4), gt=0)
    generator_lr_range: Tuple[float, float] = Field(default=(1e-5, 1e-3), gt=0)
    discriminator_lr_range: Tuple[float, float] = Field(default=(1e-5, 1e-3), gt=0)

    def _get_trial_config(self, trial: optuna.Trial) -> dict:
        return self.create_config(
            epochs=trial.suggest_categorical("epochs", choices=self.epochs_choices),
            batch_size=trial.suggest_categorical(
                "batch_size", choices=self.batch_size_choices
            ),
            generator_lr=trial.suggest_float(
                "generator_lr", *self.generator_lr_range, log=True
            ),
            discriminator_lr=trial.suggest_float(
                "discriminator_lr", *self.discriminator_lr_range, log=True
            ),
            num_layers=trial.suggest_int("num_layers", *self.num_layers_range),
            layer_width=trial.suggest_categorical(
                "layer_width", choices=self.layer_width_choices
            ),
        )

    def create_config(
        self,
        epochs: int,
        batch_size: int,
        generator_lr: float,
        discriminator_lr: float,
        num_layers: int,
        layer_width: int,
    ) -> dict:
        c = deepcopy(self.base_model_config)
        c["models"][0]["actgan"]["params"]["epochs"] = epochs
        c["models"][0]["actgan"]["params"]["batch_size"] = batch_size
        c["models"][0]["actgan"]["params"]["generator_lr"] = generator_lr
        c["models"][0]["actgan"]["params"]["discriminator_lr"] = discriminator_lr
        c["models"][0]["actgan"]["params"]["generator_dim"] = [
            layer_width for _ in range(num_layers)
        ]
        c["models"][0]["actgan"]["params"]["discriminator_dim"] = [
            layer_width for _ in range(num_layers)
        ]
        return c
