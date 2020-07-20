from dataclasses import dataclass
from numbers import Number
from typing import Tuple, Optional, Union, List

from gretel_client.transformers.base import TransformerConfig, Transformer
import re

try:
    from re import Pattern
except ImportError:
    from typing import Pattern


@dataclass(frozen=True)
class FormatConfig(TransformerConfig):
    pattern: Union[str, Pattern] = None
    replacement: str = None


class Format(Transformer):
    """
    Formatter transformer replaces matching parts of regex pattern with specified replacement.
    """
    config_class = FormatConfig

    def __init__(self, config: FormatConfig):
        super().__init__(config=config)
        if isinstance(config.pattern, Pattern):
            self.pattern = config.pattern
        else:
            self.pattern = re.compile(config.pattern)
        self.replacement = config.replacement

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        return None, self.mutate(value)

    def _transform_field(self, field: str, value: Union[Number, str], field_meta):
        return {field: self.mutate(value)}

    def mutate(self, value: str):
        return re.sub(self.pattern, self.replacement, value)
