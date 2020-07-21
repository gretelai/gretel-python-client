from dataclasses import dataclass
from numbers import Number
from typing import Optional, Tuple, Union

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

    def _transform_field(self, field_name: str, field_value: Union[Number, str], field_meta):
        return None

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        return None, ''
