from dataclasses import dataclass
from numbers import Number
from typing import Union, Optional, Tuple

from gretel_client.transformers.base import TransformerConfig, Transformer


@dataclass(frozen=True)
class RedactWithCharConfig(TransformerConfig):
    """Redact a string with a constant character. Alphanumeric characters will
    be redacted.

    Args:
        char: The character to redact with
    """
    char: str = 'X'


class RedactWithChar(Transformer):
    config_class = RedactWithCharConfig

    def __init__(self, config: RedactWithCharConfig):
        super().__init__(config)
        self.char = config.char

    def _transform_field(self, field_name: str, field_value: Union[Number, str], field_meta):
        return {field_name: mutate(field_value, char=self.char)}

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        return 'REDACTED_' + label, mutate(value, char=self.char)


def mutate(in_data: Union[Number, str], char='X'):
    if isinstance(in_data, Number):
        in_data = str(in_data)
    return ''.join((char if c.isalnum() else c for c in in_data))
