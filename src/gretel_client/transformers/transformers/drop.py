from dataclasses import dataclass
from numbers import Number
from typing import Union

from gretel_client.transformers.base import Transformer, TransformerConfig


@dataclass(frozen=True)
class DropConfig(TransformerConfig):
    """Drop a field. This transformer drops any field that gets matched in a ``DataPath`` that
    uses this transform.

    Args:
        None
    """

    pass


class Drop(Transformer):
    config_class = DropConfig

    def __init__(self, config: DropConfig):
        super().__init__(config)

    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        raise ValueError(
            "_transform method was called even though the Drop transformer does not implement this!"
        )
