from dataclasses import dataclass
from numbers import Number
from typing import Union
from gretel_client.transformers.masked import MaskedTransformer, MaskedTransformerConfig
from gretel_client.transformers.transformers.fpe_base import FpeBase, FpeBaseConfig

FPE_XFORM_CHAR = '0'


@dataclass(frozen=True)
class FpeStringConfig(MaskedTransformerConfig, FpeBaseConfig):
    """
    FpeString transformer applies a format preserving encryption as defined by https://www.nist.gov/ to the data value.
    The encryption works on strings and float values. The result is stateless and given the correct key, the original
    value can be restored.
    """
    """
     Args:
        radix: Base from 2 to 62, determines base of incoming data types. Base2 = binary, Base62 = alphanumeric 
        including upper and lower case characters.
        secret: 256bit AES encryption string specified as 64 hexadecimal characters.
        mask: String to specify a mask of which characters should be transformed. Any character in the mask that is
        specified as '0' will be encrypted in the data string. E.g.: mask='0011' value='1234' -> 'EE34' ('E'ncrypted)
        float_precision: This value only matters if the incoming data value is of type float.
     NOTE:
         Example configuration:
     """


class FpeString(MaskedTransformer, FpeBase):
    config_class = FpeStringConfig

    def __init__(self, config: FpeStringConfig):
        super().__init__(config)

    def _transform(self, value) -> Union[Number, str]:
        return FpeBase._transform(self, value)

    def _restore(self, value) -> Union[Number, str]:
        return FpeBase._restore(self, value)
