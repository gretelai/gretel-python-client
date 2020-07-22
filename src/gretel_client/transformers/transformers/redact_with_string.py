from dataclasses import dataclass
from numbers import Number
from typing import Union

from gretel_client.transformers.base import TransformerConfig, Transformer


@dataclass(frozen=True)
class RedactWithStringConfig(TransformerConfig):
    string: str = None


class RedactWithString(Transformer):
    """
    RedactWithString transformer replaces text with a string representation of the field/label it occurs in.
    """
    config_class = RedactWithStringConfig

    def __init__(self, config: RedactWithStringConfig):
        super().__init__(config)
        self.string = config.string

    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        return self.string