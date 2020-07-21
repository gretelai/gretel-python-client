from dataclasses import dataclass
from numbers import Number
from typing import Union, Optional, Tuple

from gretel_client.transformers.base import TransformerConfig, Transformer


@dataclass(frozen=True)
class RedactWithStringConfig(TransformerConfig):
    """Redact a string with another custom string.

    Args:
        string: The string to replace the entire matched span with.

    NOTE:
        This will redact an entire field or entire matched entity, it does not go char by char
    """
    string: str = None


class RedactWithString(Transformer):
    config_class = RedactWithStringConfig

    def __init__(self, config: RedactWithStringConfig):
        super().__init__(config)
        self.string = config.string

    def _transform_field(self, field_name: str, field_value: Union[Number, str], field_meta):
        return {field_name: self.string}

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        return 'REDACTED_' + label, self.string
