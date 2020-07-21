from dataclasses import dataclass
from numbers import Number
from typing import Tuple, Optional, Union
import re

from gretel_client.transformers.base import TransformerConfig, Transformer

try:
    from re import Pattern
except ImportError:
    from typing import Pattern


@dataclass(frozen=True)
class FormatConfig(TransformerConfig):
    """Modify the contents of a string by specifying a regular expression that matches sub-strings
    and a replacement character for any characters that match. Generally, this is useful for removing
    special or un-needed chars from a field or value

    Args:
        pattern: A regex string to match characters against
        replacement: A string to replace each matching character / string with

    Example to remove non-numerical chars::

        config = FormatConfig(pattern=r"[^\\d]", replacement="")
    """
    pattern: Union[str, Pattern] = None
    replacement: str = None


class Format(Transformer):
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
