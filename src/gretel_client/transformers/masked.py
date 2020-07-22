from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import singledispatch
from numbers import Number
from typing import Union, Optional, Tuple, List

from gretel_client.transformers.string_mask import StringMask

FPE_XFORM_CHAR = '0'


@dataclass(frozen=True)
class MaskedTransformerConfig(ABC):
    """
    FpeBase transformer applies a format preserving encryption as defined by https://www.nist.gov/ to the data value.
    The encryption works on strings and float values. The result is stateless and given the correct key, the original
    value can be restored.
    """
    mask: List[StringMask] = None
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


@singledispatch
def revert_str_to_type(type_data, data):
    raise ValueError(type_data)


@revert_str_to_type.register(str)
def _(type_data, data):
    return data


@revert_str_to_type.register(float)
def _(type_data, data):
    return float(data)


@revert_str_to_type.register(int)
def _(type_data, data):
    return int(data)


class MaskedTransformer(ABC):
    config_class = MaskedTransformerConfig

    def __init__(self, config: MaskedTransformerConfig):
        super().__init__(config)
        self.mask = config.mask or [StringMask()]

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        return label, self._mask_value(value, None, self._transform)

    def _transform_field(self, field: str, value: Union[Number, str], field_meta):
        return {field: self._mask_value(value, field_meta, self._transform)}

    def _mask_value(self, value: Union[Number, str], field_meta, _value_func):
        _value = value if isinstance(value, str) else str(value)
        if self.mask:
            for mask in reversed(self.mask):
                masked_value, m_slice = mask.get_masked_chars_slice(_value)
                new_value = _value_func(masked_value)
                _value = _value[:m_slice.start] + new_value[:] + (_value[m_slice.stop:] if m_slice.stop else '')
        _value = revert_str_to_type(value, _value)
        return _value

    @abstractmethod
    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        pass
