from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from numbers import Number
from typing import Union

from dateparser.date import DateDataParser

from gretel_client.transformers.base import FieldRef
from gretel_client.transformers.fpe.crypto_aes import Mode
from gretel_client.transformers.fpe.fpe_ff1 import FpeFf1
from gretel_client.transformers.restore import (
    RestoreTransformer,
    RestoreTransformerConfig,
)
from gretel_client.transformers.transformers import fpe_base


@dataclass(frozen=True)
class DateShiftConfig(RestoreTransformerConfig):
    """Adjust a date/time string within a certain amount of bounds, using FPE.

    Args:
        lower_range_days: The maximum number of days to adjust backwards in time.
        upper_range_days: The maximum number of days to adjust forwards in time.
        secret: Encryption key to use.
        tweak: Optionally base the time shift on another field in the record.
        date_format: (Optional) Specify desired input/output format in https://strftime.org/ syntax. If None, then the
            output format will always be formatted to "%m/%d/%Y". If you specify a date_format string, it will first
            attempt to read the incoming value using this date format and fall back to a generic date parser if it
            fails. The output format however, will always be the one specified here or the default.
    """

    lower_range_days: int = None
    upper_range_days: int = None
    secret: str = None
    tweak: FieldRef = None
    date_format: str = None


class DateShift(RestoreTransformer):
    config_class = DateShiftConfig

    def __init__(self, config: DateShiftConfig):
        super().__init__(config=config)
        self._fpe_ff1 = FpeFf1(
            radix=10,
            maxTLen=0,
            key=bytearray.fromhex(config.secret),
            tweak=b"",
            mode=Mode.CBC,
        )
        self.lower_range_days = config.lower_range_days
        self.upper_range_days = config.upper_range_days
        self.range = config.upper_range_days - config.lower_range_days
        if self.range < 1:
            raise ValueError("Lower/upper range needs to be greater than 1")
        self.format = config.date_format

    def _get_date_delta(self, date_val: str):
        field_ref = self._get_field_ref("tweak")
        if field_ref:
            tweak, _ = fpe_base.cleanup_value(field_ref.value, field_ref.radix)
            tweak = str(tweak).zfill(16)
            tweak_val = self._fpe_ff1.encrypt(tweak.encode(), field_ref.radix)
        else:
            tweak = "0000000000000000"
            tweak_val = self._fpe_ff1.encrypt(tweak.encode())

        tweak_val = self._fpe_ff1.decode(tweak_val)
        days = int(tweak_val) % self.range + self.lower_range_days
        _date_val = None
        if self.format:
            try:
                _date_val = datetime.strptime(date_val, self.format).date()
            except ValueError:
                pass
        if not _date_val:
            _date_val = DateDataParser(settings={"STRICT_PARSING": True}).get_date_data(
                date_val
            )
            _date_val = _date_val["date_obj"].date()
        return days, _date_val

    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        try:
            days, date_val = self._get_date_delta(value)
        except Exception:
            return value
        date_val += timedelta(days=days)
        if self.format:
            return date_val.strftime(self.format)
        else:
            return str(date_val)

    def _restore(self, value: Union[Number, str]) -> Union[Number, str]:
        try:
            days, date_val = self._get_date_delta(value)
        except Exception:
            return value
        date_val -= timedelta(days=days)
        if self.format:
            return date_val.strftime(self.format)
        else:
            return str(date_val)
