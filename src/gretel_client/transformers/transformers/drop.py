from dataclasses import dataclass
from numbers import Number
from typing import Union

from gretel_client.transformers.base import Transformer, TransformerConfig


@dataclass(frozen=True)
class DropConfig(TransformerConfig):
    pass


class Drop(Transformer):
    """
    Drop transformer completely removes an entity or field (text and semantic metadata).
    """
    config_class = DropConfig

    def __init__(self, config: DropConfig):
        super().__init__(config)

    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        raise ValueError(
            "_transform method was called even though the Drop transformer does not implement this!")

