from dataclasses import dataclass
from decimal import Decimal
from numbers import Number
import re
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
            The return type from Faker is retained and used to replace the source value.
            Please see the ``faker`` module docs for the available methods to use.
        locales: A list of locales to use for generating fake values. Please see the ``faker`` module docs
            for the available locales to use.
        locale_seed: An optional seed to use for initializing the order of locales to be used for creating fake values
    """

    seed: int = None
    fake_method: str = None
    locales: List[str] = None
    locale_seed: Union[int, None] = 0


class FakeConstant(Transformer):
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

    def _transform_entity(
        self, label: str, value: Union[Number, str]
    ) -> Optional[Tuple[Optional[str], str]]:
        fake_method = FAKER_MAP.get(label)
        if fake_method is None:
            return None
        new_value = self.faker.constant_fake(value, fake_method)

        # Convert Faker Decimal return type to Python float
        if isinstance(new_value, Decimal):
            new_value = float(new_value)
        return None, new_value

    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        return self.faker.constant_fake(value, self.fake_method)
