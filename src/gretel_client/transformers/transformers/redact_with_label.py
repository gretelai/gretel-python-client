from dataclasses import dataclass
from numbers import Number
from typing import Tuple, Optional, Union
from gretel_client.transformers.base import TransformerConfig, Transformer


@dataclass(frozen=True)
class RedactWithLabelConfig(TransformerConfig):
    pass


class RedactWithLabel(Transformer):
    """
    RedactWithLabel transformer replaces an entity text with a string representation of its label.
    """
    config_class = RedactWithLabelConfig

    def _transform_field(self, field_name: str, field_value: Union[Number, str], field_meta):
        if field_meta is not None:
            try:
                label = field_meta['ner']['labels'][0]['label']
            except KeyError:
                return None
            else:
                return {field_name: label.upper()}
        else:
            return None

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        return None, label.upper()
