from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import singledispatch
from numbers import Number
from typing import Union, Optional, Tuple, List

from gretel_client.transformers.string_mask import StringMask

FPE_XFORM_CHAR = '0'

# NOTE(jm): Internally used, no docstrings required


@dataclass(frozen=True)
class MaskedTransformerConfig(ABC):
    mask: List[StringMask] = None


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
        if value is None:
            return {field: value}
        if isinstance(value, str) and not value:
            return {field: value}
        return {field: self._mask_value(value, field_meta, self._transform)}

    def _mask_value(self, value: Union[Number, str], field_meta, _value_func):
        _value = value if isinstance(value, str) else str(value)
        if self.mask:
            for mask in reversed(self.mask):
                masked_value, m_slice = mask.get_masked_chars_slice(_value)
                new_value = _value_func(masked_value)
                _value = _value[:m_slice.start] + new_value[:] + (_value[m_slice.stop:] if m_slice.stop else '')
        try:
            _value = revert_str_to_type(value, _value)
        # if the value cannot be reverted to the original value (ex: float 1.23 -> x.xx with char_redaction)
        except ValueError:
            pass
        return _value

    @abstractmethod
    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        pass
