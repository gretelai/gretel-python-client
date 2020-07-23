from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import singledispatch
from numbers import Number
from typing import Union, Optional, Tuple

from gretel_client.transformers.masked import MaskedTransformerConfig, MaskedTransformer

FPE_XFORM_CHAR = '0'


# NOTE(jm): no docstrings required, not user facing


@dataclass(frozen=True)
class MaskedRestoreTransformerConfig(MaskedTransformerConfig, ABC):
    pass


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


class MaskedRestoreTransformer(MaskedTransformer, ABC):
    config_class = MaskedRestoreTransformerConfig

    def __init__(self, config: MaskedTransformerConfig):
        super().__init__(config)

    def _restore_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        return label, self._mask_value(value, None, self._restore)

    def _restore_field(self, field: str, value: Union[Number, str], field_meta):
        return {field: self._mask_value(value, field_meta, self._restore)}

    @abstractmethod
    def _restore(self, value) -> Union[Number, str]:
        pass
