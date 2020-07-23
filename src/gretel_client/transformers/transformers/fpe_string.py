from dataclasses import dataclass
from numbers import Number
from typing import Union

from gretel_client.transformers.masked_restore import (
    MaskedRestoreTransformerConfig,
    MaskedRestoreTransformer,
)
from gretel_client.transformers.transformers.fpe_base import FpeBase, FpeBaseConfig

FPE_XFORM_CHAR = "0"


@dataclass(frozen=True)
class FpeStringConfig(MaskedRestoreTransformerConfig, FpeBaseConfig):
    """
    FpeString transformer applies a format preserving encryption as defined by https://www.nist.gov/ to the data value.
    The encryption works on strings. The result is stateless and given the correct key, the original
    value can be restored.

    Args:
        radix: Base from 2 to 62, determines base of incoming data types. Base2 = binary, Base62 = alphanumeric
            including upper and lower case characters.
        secret: 256bit AES encryption string specified as 64 hexadecimal characters.
        mask: An optional list of ``StringMask`` objects. If provided only the parts of the string defined by the masks
            will be encrypted.
     """


class FpeString(MaskedRestoreTransformer, FpeBase):
    config_class = FpeStringConfig

    def __init__(self, config: FpeStringConfig):
        super().__init__(config)

    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        return FpeBase._transform(self, value)

    def _restore(self, value: Union[Number, str]) -> Union[Number, str]:
        return FpeBase._restore(self, value)
