import hashlib
import hmac
from dataclasses import dataclass
from numbers import Number
from typing import Union

from gretel_client.transformers.base import TransformerConfig, Transformer


@dataclass(frozen=True)
class SecureHashConfig(TransformerConfig):
    """Replace a string with a HMAC derived from that string and a secret key.

    Args:
        secret: The encryption key to use for the HMAC
    """
    secret: str = None


class SecureHash(Transformer):
    config_class = SecureHashConfig

    def __init__(self, config: SecureHashConfig):
        super().__init__(config)
        self.secret = config.secret.encode()

    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        return hmac.new(self.secret, str(value).encode(), hashlib.sha256).hexdigest()
