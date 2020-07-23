from dataclasses import dataclass
from numbers import Number
from typing import Union

from gretel_client.transformers.base import TransformerConfig, Transformer
from gretel_client.transformers.masked import MaskedTransformerConfig, MaskedTransformer


@dataclass(frozen=True)
class RedactWithCharConfig(MaskedTransformerConfig, TransformerConfig):
    """Redact a string with a constant character. Alphanumeric characters will
    be redacted.

    Args:
        char: The character to redact with
        mask: An optioan list of ``StringMask`` objects. If provided, only the parts of the string defined
            in the masks will be redacted.

    """
    char: str = 'X'


class RedactWithChar(MaskedTransformer, Transformer):
    config_class = RedactWithCharConfig

    def __init__(self, config: RedactWithCharConfig):
        super().__init__(config)
        self.char = config.char

    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        if isinstance(value, Number):
            value = str(value)
        return ''.join((self.char if c.isalnum() else c for c in value))
