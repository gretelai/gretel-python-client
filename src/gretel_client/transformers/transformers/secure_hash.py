import hashlib
import hmac
from dataclasses import dataclass
from numbers import Number
from typing import Optional, Tuple, Union

from gretel_client.transformers.base import TransformerConfig, Transformer


@dataclass(frozen=True)
class SecureHashConfig(TransformerConfig):
    secret: str = None


class SecureHash(Transformer):
    """
    SecureHash transformer replaces a value with its hash based on a secret.
    """
    config_class = SecureHashConfig

    def __init__(self, config: SecureHashConfig):
        super().__init__(config)
        self.secret = config.secret.encode()

    def _transform_field(self, field_name: str, field_value: Union[Number, str], field_meta):
        return {field_name: self._transform(field_value)}

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        return None, self._transform(value)

    def _transform(self, in_data: str):
        return hmac.new(self.secret, str(in_data).encode(), hashlib.sha256).hexdigest()
