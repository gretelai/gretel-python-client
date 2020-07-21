from dataclasses import dataclass
from numbers import Number
from typing import Union, Optional, Tuple

from gretel_client.transformers.transformers.fpe_base import FpeBase, FpeBaseConfig, cleanup_value, dirtyup_value


@dataclass(frozen=True)
class FpeFloatConfig(FpeBaseConfig):
    """
    FpeFloat transformer applies a format preserving encryption (FPE) as defined by https://www.nist.gov/ to the data
    value. The encryption works on strings and float values. Strings get treated as numerical values before the FPE
    transform gets applied. The result is stateless and given the correct key, the original value can be restored.
    """
    float_precision: int = None
    """
     Args:
        radix: Base from 2 to 62, determines base of incoming data types. Base2 = binary, Base62 = alphanumeric 
        including upper and lower case characters.
        secret: 256bit AES encryption string specified as 64 hexadecimal characters.
        specified as '0' will be encrypted in the data string. E.g.: mask='0011' value='1234' -> 'EE34' ('E'ncrypted)
        float_precision: This value only matters if the incoming data value is of type float.
     NOTE:
         Example configuration:
     """


class FpeFloat(FpeBase):
    config_class = FpeFloatConfig

    def __init__(self, config: FpeFloatConfig):
        super().__init__(config)
        self.float_precision = config.float_precision

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        clean, dirt_mask = cleanup_value(value, self.radix)
        if isinstance(value, float):
            return label, dirtyup_value(self._fpe_ff1.encrypt((clean, self.float_precision)), dirt_mask)
        else:
            return label, str(dirtyup_value(self._fpe_ff1.encrypt((float(clean), self.float_precision)), dirt_mask))

    def _restore_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        clean, dirt_mask = cleanup_value(value, self.radix)
        if isinstance(value, float):
            return label, dirtyup_value(self._fpe_ff1.decrypt((clean, self.float_precision)), dirt_mask)
        else:
            return label, str(dirtyup_value(self._fpe_ff1.decrypt((float(clean), self.float_precision)), dirt_mask))

    def _transform_field(self, field: str, value: Union[Number, str], field_meta):
        clean, dirt_mask = cleanup_value(value, self.radix)
        if isinstance(value, float):
            return {field: dirtyup_value(self._fpe_ff1.encrypt((clean, self.float_precision)), dirt_mask)}
        else:
            return {field: str(dirtyup_value(self._fpe_ff1.encrypt((float(clean), self.float_precision)), dirt_mask))}

    def _restore_field(self, field, value: Union[Number, str], field_meta):
        clean, dirt_mask = cleanup_value(value, self.radix)
        if isinstance(value, float):
            return {field: dirtyup_value(self._fpe_ff1.decrypt((clean, self.float_precision)), dirt_mask)}
        else:
            return {field: str(dirtyup_value(self._fpe_ff1.decrypt((float(clean), self.float_precision)), dirt_mask))}

