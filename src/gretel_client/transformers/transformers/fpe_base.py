import re
import struct
from dataclasses import dataclass
from functools import singledispatch
from math import log2, ceil
from numbers import Number
from typing import Union, Optional, Tuple

from gretel_client.transformers.fpe.crypto_aes import Mode
from gretel_client.transformers.fpe.fpe_ff1 import FpeFf1
from gretel_client.transformers.restore import RestoreTransformerConfig, RestoreTransformer

FPE_XFORM_CHAR = '0'


@dataclass(frozen=True)
class FpeBaseConfig(RestoreTransformerConfig):
    """
    FpeBase transformer applies a format preserving encryption as defined by https://www.nist.gov/ to the data value.
    The encryption works on strings and float values. The result is stateless and given the correct key, the original
    value can be restored.
    """
    radix: int = None
    secret: str = None
    float_precision: int = None
    aes_mode: Mode = Mode.CBC
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


class FpeBase(RestoreTransformer):
    config_class = FpeBaseConfig

    def __init__(self, config: FpeBaseConfig):
        super().__init__(config)
        self._fpe_ff1 = FpeBaseFf1Common(config.secret, config.radix, config.aes_mode)
        self.radix = config.radix
        self.float_precision = config.float_precision

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        clean, dirt_mask, fpe_mask = self._clean_and_mask_value(value)
        dirty = self._dirty_and_un_mask_value(self._fpe_ff1.encrypt(clean), dirt_mask, fpe_mask)
        return label, dirty

    def _restore_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        clean, dirt_mask, fpe_mask = self._clean_and_mask_value(value)
        dirty = self._dirty_and_un_mask_value(self._fpe_ff1.decrypt(clean), dirt_mask, fpe_mask)
        return label, dirty

    def _clean_and_mask_value(self, value):
        fpe_mask = None
        clean, dirt_mask = cleanup_value(value, self.radix)
        return clean, dirt_mask, fpe_mask

    def _dirty_and_un_mask_value(self, value, dirt_mask, fpe_mask):
        dirty = dirtyup_value(value, dirt_mask)
        if fpe_mask:
            it = iter(dirty)
            dirty = ''.join(map(lambda v: next(it) if (v == '\0') else v, fpe_mask))
        return dirty

    def _transform(self, value) -> Union[Number, str]:
        clean, dirt_mask, fpe_mask = self._clean_and_mask_value(value)
        return self._dirty_and_un_mask_value(self._fpe_ff1.encrypt(clean), dirt_mask, fpe_mask)

    def _restore(self, value) -> Union[Number, str]:
        clean, dirt_mask, fpe_mask = self._clean_and_mask_value(value)
        return self._dirty_and_un_mask_value(self._fpe_ff1.decrypt(clean), dirt_mask, fpe_mask)


RADIX_TO_CLEAN_STR_DICT = {
    2: re.compile('[^0-1]'),
    10: re.compile('[^0-9]'),
    16: re.compile('[^0-9a-fA-F]'),
    36: re.compile('[^0-9a-z]'),
    62: re.compile('[^0-9a-zA-Z]'),
    85: re.compile(r'[^0-9a-zA-Z!#$%&()*+\-;<=>?@^_`{|}~]'),
    94: re.compile(r'[^0-9a-zA-Z!#$%&()*+\-;<=>?@^_`{|}~"'"'"r',./:\[\]\\]')
}

RADIX_TO_DIRTY_STR_DICT = {
    2: re.compile('[0-1]'),
    10: re.compile('[0-9]'),
    16: re.compile('[0-9a-fA-F]'),
    36: re.compile('[0-9a-z]'),
    62: re.compile('[0-9a-zA-Z]'),
    85: re.compile(r'[0-9a-zA-Z!#$%&()*+\-;<=>?@^_`{|}~]'),
    94: re.compile(r'[0-9a-zA-Z!#$%&()*+\-;<=>?@^_`{|}~"' "'" r',./:\[\]\\]')
}


class FpeBaseFf1Common:
    def __init__(self, secret_key: str, radix: int, mode: Mode):
        self._cipher = FpeFf1(radix=radix,
                              maxTLen=0,
                              key=bytes.fromhex(secret_key),
                              tweak=b'',
                              mode=mode)

    def encrypt(self, in_data):
        return _cipher_ff1_fpe(in_data, FpeFf1.encrypt, self._cipher)

    def decrypt(self, in_data):
        return _cipher_ff1_fpe(in_data, FpeFf1.decrypt, self._cipher)


# define double_to_hex (or float_to_hex)
def double_to_hex(f):
    return hex(struct.unpack('<Q', struct.pack('<d', f))[0])


def double_to_bin(f):
    return bin(struct.unpack('<Q', struct.pack('<d', f))[0])


def float_to_hex(f):
    return hex(struct.unpack('<I', struct.pack('<f', f))[0])


def float_to_bin(f):
    return bin(struct.unpack('<I', struct.pack('<f', f))[0])


@singledispatch
def _cipher_ff1_fpe(in_data, cipher_func, _cipher: FpeFf1):
    raise ValueError(in_data)


@_cipher_ff1_fpe.register(str)
def _(in_data: str, cipher_func, _cipher: FpeFf1):
    encoded_text = in_data.encode()
    cipher_data = cipher_func(_cipher, encoded_text)
    return cipher_data.decode()


@_cipher_ff1_fpe.register(float)
def _(in_data: float, cipher_func, _cipher: FpeFf1):
    in_data_str = double_to_bin(in_data)
    mantissa = in_data_str[-52:]
    exp = in_data_str[2:-52]
    cipher_data = cipher_func(_cipher, mantissa.encode(), 2).decode()
    exp = exp.zfill(12)
    cipher_data_str = '0b' + exp + cipher_data
    cipher_data = struct.unpack('d', struct.pack('Q', int(cipher_data_str, 0)))[0]
    return cipher_data


@_cipher_ff1_fpe.register(tuple)
def _(in_data: tuple, cipher_func, _cipher: FpeFf1):
    precision = in_data[1]
    # binary string is '0b(sign)(exponent)(mantissa), ditch '0b' and expand to 64bit in case exp is not 12bits
    in_data_str = double_to_bin(in_data[0])[2:].zfill(64)
    exp = in_data_str[:-52]
    mantissa = in_data_str[-52:]
    int_log = int(abs(in_data[0]))
    if int_log > 0:
        int_log = int(log2(int_log))
    log2_bit_offset = int_log + ceil(log2(10 ** precision))
    if log2_bit_offset > 50:
        return in_data[0]
    start_bit = -52 + log2_bit_offset
    mod_mantissa = in_data_str[start_bit:]
    cipher_data = cipher_func(_cipher, mod_mantissa.encode(), 2).decode()
    cipher_data = mantissa[:start_bit] + cipher_data[:]
    cipher_data_str = '0b' + exp + cipher_data
    cipher_data = struct.unpack('d', struct.pack('Q', int(cipher_data_str, 0)))[0]

    return cipher_data


@_cipher_ff1_fpe.register(int)
def _(in_data: int, cipher_func, _cipher: FpeFf1):
    in_data_str = str(in_data)
    negative = in_data_str.startswith('-')
    stripped_text = in_data_str.replace('-', '')
    if len(stripped_text) < 2:
        return in_data
    stripped_mid = stripped_text[1:]
    cipher_data = cipher_func(_cipher, stripped_mid.encode())
    cipher_data = stripped_text[0] + cipher_data.decode()
    if negative:
        cipher_data = '-' + cipher_data
    cipher_data = int(cipher_data)
    return cipher_data


@singledispatch
def cleanup_value(dirty, radix: int):
    raise ValueError(dirty)


@cleanup_value.register(str)
def _(dirty: str, radix: int):
    clean = RADIX_TO_CLEAN_STR_DICT[radix].sub('', dirty)
    dirty_mask = RADIX_TO_DIRTY_STR_DICT[radix].sub('\0', dirty)
    return clean, dirty_mask


@cleanup_value.register(float)
def _(dirty: float, radix: int):
    return dirty, ''


@cleanup_value.register(int)
def _(dirty: int, radix: int):
    return dirty, ''


@singledispatch
def dirtyup_value(clean, dirty_mask: str):
    raise ValueError(clean)


@dirtyup_value.register(str)
def _(clean: str, dirty_mask):
    it = iter(clean)
    return ''.join(map(lambda v: next(it) if (v == '\0') else v, dirty_mask))


@dirtyup_value.register(float)
def _(clean: float, dirty_mask):
    return clean


@dirtyup_value.register(int)
def _(clean: int, dirty_mask):
    return clean
