from dataclasses import dataclass
from numbers import Number
from typing import Optional, Tuple, Union

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

    def _transform_field(self, field_name: str, field_value: Union[Number, str], field_meta):
        return None

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        return None, ''
