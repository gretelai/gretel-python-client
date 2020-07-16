from dataclasses import dataclass
from numbers import Number
from typing import Optional, Tuple, Union

from gretel_client.transformers.base import Transformer, TransformerConfig, FieldRef


@dataclass(frozen=True)
class CombineConfig(TransformerConfig):
    combine: FieldRef = None
    separator: str = None


class Combine(Transformer):
    """
    Combine transformer combines multiple fields into one output field separated by separator string specified.
    """
    config_class = CombineConfig

    def __init__(self, config: CombineConfig):
        super().__init__(config=config)
        self.separator = config.separator or ""

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        return None, self.mutate(value)

    def _transform_field(self, field: str, value: Union[Number, str], field_meta):
        return {field: self.mutate(value)}

    def _get_combiners(self):
        combine_list = self._get_field_ref('combine')
        return combine_list.value

    def mutate(self, value: str):
        return value + self.separator + self.separator.join(self._get_combiners())
