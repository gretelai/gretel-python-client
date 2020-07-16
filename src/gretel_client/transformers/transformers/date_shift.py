from dataclasses import dataclass
from datetime import timedelta
from numbers import Number
from typing import Optional, Tuple, Union

from dateparser.date import DateDataParser

from gretel_client.transformers.base import FieldRef
from gretel_client.transformers.fpe.crypto_aes import Mode
from gretel_client.transformers.fpe.fpe_ff1 import FpeFf1
from gretel_client.transformers.restore import RestoreTransformer, RestoreTransformerConfig
from gretel_client.transformers.transformers import secure_fpe


@dataclass(frozen=True)
class DateShiftConfig(RestoreTransformerConfig):
    lower_range_days: int = None
    upper_range_days: int = None
    secret: str = None
    tweak: FieldRef = None
    aes_mode: Mode = Mode.CBC


class DateShift(RestoreTransformer):
    """
    DateShift transformer applies a reversible date shift based on a 256bit AES key, that can be "tweaked" by another
    field of the same record.
    """
    config_class = DateShiftConfig

    def __init__(self, config: DateShiftConfig):
        super().__init__(config=config)
        self._fpe_ff1 = FpeFf1(radix=10,
                               maxTLen=0,
                               key=bytearray.fromhex(config.secret),
                               tweak=b'',
                               mode=config.aes_mode)
        self.lower_range_days = config.lower_range_days
        self.upper_range_days = config.upper_range_days
        self.range = config.upper_range_days - config.lower_range_days
        if self.range < 1:
            raise ValueError

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        return None, self.mutate(value)

    def _restore_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        return None, self.restore(value)

    def _transform_field(self, field: str, value: Union[Number, str], field_meta):
        return {field: self.mutate(value)}

    def _restore_field(self, field, value: Union[Number, str], field_meta):
        return {field: self.restore(value)}

    def _get_date_delta(self, date_val: str):
        field_ref = self._get_field_ref('tweak')
        if field_ref:
            tweak, _ = secure_fpe.cleanup_value(field_ref.value, field_ref.radix)
            tweak = str(tweak).zfill(16)
            tweak_val = self._fpe_ff1.encrypt(tweak.encode(), field_ref.radix)
        else:
            tweak = '0000000000000000'
            tweak_val = self._fpe_ff1.encrypt(tweak.encode())

        tweak_val = self._fpe_ff1.decode(tweak_val)
        days = int(tweak_val) % self.range + self.lower_range_days
        date_val = DateDataParser(settings={'STRICT_PARSING': True}).get_date_data(date_val)
        date_val = date_val['date_obj'].date()
        return days, date_val

    def mutate(self, date_val: str):
        days, date_val = self._get_date_delta(date_val)
        date_val += timedelta(days=days)
        return str(date_val)

    def restore(self, date_val: str):
        days, date_val = self._get_date_delta(date_val)
        date_val -= timedelta(days=days)
        return str(date_val)
