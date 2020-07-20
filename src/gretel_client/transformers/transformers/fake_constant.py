from dataclasses import dataclass
from numbers import Number
from typing import Tuple, Optional, Union, List

from gretel_client.transformers.base import TransformerConfig, Transformer
from gretel_client.transformers.fakers import _Fakers, FAKER_MAP

VALID_LABELS = [key for key, value in FAKER_MAP.items() if value]


@dataclass(frozen=True)
class FakeConstantConfig(TransformerConfig):
    """Create a fake value of a certain entity / type given an input value.

    Args:
        seed: A required starting seed for the underlying fake generator. Use the same seed in order
            to get the same fake values for a given input.
        fake_method: A string of what kind of fake entity to create. One of the keys from the ``FAKER_MAP`` mapping.
        locales: A list of locales to use for generating fake values
        locale_seed: An optional seed to use for init'ing the order of locales to be used for creating fake values
    """
    seed: int = None
    fake_method: str = None
    locales: List[str] = None
    locale_seed: Union[int, None] = 0


class FakeConstant(Transformer):
    """
    FakeConstant transformer replaces the value with a new fake value.
    """

    config_class = FakeConstantConfig

    def __init__(self, config: FakeConstantConfig):
        super().__init__(config)
        self.faker = _Fakers(
            seed=config.seed, locales=config.locales, locale_seed=config.locale_seed
        )
        self.fake_method = config.fake_method
        if self.labels:
            if not all(elem in VALID_LABELS for elem in self.labels):
                raise ValueError(
                    f"Labels list has to be one or more of {VALID_LABELS} only!"
                )
        elif not self.fake_method:
            raise ValueError(
                "No fake methods or labels which map to fake methods are specified!"
            )

    def _transform_field(self, field: str, value: Union[Number, str], field_meta):
        return {field: self.mutate(value, self.fake_method)}

    def _transform_entity(
        self, label: str, value: Union[Number, str]
    ) -> Optional[Tuple[Optional[str], str]]:
        fake_method = FAKER_MAP.get(label)
        if fake_method is None:
            return None
        new_value = self.mutate(value, fake_method)
        if isinstance(value, float):
            new_value = float(new_value)
        elif isinstance(value, str):
            new_value = str(new_value)
        return None, new_value

    def mutate(self, value, fake_method: str):
        return self.faker.constant_fake(value, fake_method)
