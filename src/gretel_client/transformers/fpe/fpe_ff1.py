import math
import struct

import numpy

from gretel_client.transformers.fpe.crypto_aes import BLOCK_SIZE, AESCipher, Mode

HALF_BLOCK_SIZE = int(BLOCK_SIZE / 2)
FEISTEL_MIN = 100.0
NUM_ROUNDS = 10

BASE62_ALPH = tuple("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
BASE85_ALPH = tuple("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%&()*+-;<=>?@^_`{|}~")
BASE94_ALPH = tuple("""0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%&()*+-;<=>?@^_`{|}~"',"""
                    """./:[]\\""")

BASE62_DICT = dict((ord(c), v) for v, c in enumerate(BASE62_ALPH))
BASE85_DICT = dict((ord(c), v) for v, c in enumerate(BASE85_ALPH))
BASE94_DICT = dict((ord(c), v) for v, c in enumerate(BASE94_ALPH))

BASE_ALPH_MAP = {
    62: BASE62_ALPH,
    85: BASE85_ALPH,
    94: BASE94_ALPH
}

BASE_DICT_MAP = {
    62: BASE62_DICT,
    85: BASE85_DICT,
    94: BASE94_DICT
}


def rev(s: str) -> str:
    return ''.join(reversed(s))


def revB(a: bytearray) -> bytearray:
    i = int(len(a) / 2) - 1
    while i >= 0:
        opp = len(a) - 1 - i
        a[i], a[opp] = a[opp], a[i]
        i = i - 1
    return a


class CipherError(Exception):
    pass


class FpeFf1:
    def __init__(self, *, radix: int, maxTLen: int, key: bytes, tweak: bytes, mode=Mode.CBC):
        keyLen = len(key)
        if keyLen != 16 and keyLen != 24 and keyLen != 32:
            raise CipherError("Cipher error: key length must be 128, 192, or 256 bits")
        if not radix or radix < 2 or (radix > 36 and radix != 62 and radix != 85 and radix != 94):
            raise CipherError("radix must be between 2 and 36, inclusive, or one of either 62, 85, or 94")
        if len(tweak) > maxTLen:
            raise CipherError(f"Tweak size must be no greater than {maxTLen} bytes in length")
        self.minLen = int(math.ceil(math.log(FEISTEL_MIN) / math.log(float(radix))))
        self.maxLen = 1 << 32 - 1  # TODO: verify this value. This value was used in golang. "math.MaxUint32"
        if self.minLen < 2 or self.maxLen < self.minLen or float(self.maxLen) > 1 << 32 - 1:
            raise CipherError("minLen or maxLen invalid, adjust your radix")
        self.aes_block = AESCipher(key, mode)
        if self.aes_block is None:
            raise CipherError("Could not create AESCipher")
        self.tweak = tweak
        self.maxTLen = maxTLen
        self._update_radix(radix)

    def _update_radix(self, radix: int):
        self.radix = radix
        if radix in BASE_DICT_MAP:
            self._base_code_alph = BASE_ALPH_MAP[radix]
            self._base_code_dict = BASE_DICT_MAP[radix]
            self._decode_func = self._base_decode
            self._encode_func = self._base_encode
        else:
            self._decode_func = self._base_decode_int
            self._encode_func = self._base_encode_numpy

    def decode(self, value: bytes):
        return self._decode_func(value)

    def encode(self, value: int):
        return self._encode_func(value)

    @staticmethod
    def _get_trimmed_bytes(val: int) -> bytes:
        numbits = math.frexp(val)[1]
        numbytes = int((numbits + 7) / 8)
        return val.to_bytes(numbytes, byteorder='big')

    def _base_decode_int(self, bytes: bytes):
        return int(bytes, self.radix)

    def _base_encode_numpy(self, num: int):
        return numpy.base_repr(num, self.radix).lower()

    def _base_decode(self, bytes: bytes):
        num = 0
        for char in bytes:
            num = num * self.radix + self._base_code_dict[char]
        return num

    def _base_encode(self, num: int):
        if not num:
            return self._base_code_alph[0]

        encoding = ""
        while num:
            num, rem = divmod(num, self.radix)
            encoding = self._base_code_alph[rem] + encoding
        return encoding

    def encrypt(self, X: bytes, radix_override: int = None) -> (bytearray, CipherError):
        return self.encrypt_with_tweak(X, self.tweak, radix_override)

    def encrypt_with_tweak(self, X: bytes, tweak: bytes, radix_override: int) -> bytes:
        old_radix = self.radix
        if radix_override:
            self._update_radix(radix_override)
        n = len(X)
        t = len(tweak)

        if n < self.minLen or n >= self.maxLen:
            raise CipherError("message length is not within min and max bounds")

        if len(tweak) > self.maxTLen:
            raise CipherError(f"Tweak size must be no greater than {self.maxTLen} bytes in length")

        # Check if the message is in the current radix
        try:
            self._decode_func(X)
        except ValueError:
            raise CipherError(f"Input string {X} invalid for radix {self.radix}")

        # Calculate split point
        u = int(n / 2)
        v = n - u

        # / Split the message
        A = memoryview(X[:u])
        B = memoryview(X[u:])
        b = int(math.ceil(math.ceil(float(v) * math.log2(float(self.radix))) / 8))
        d = int(4 * math.ceil(float(b) / 4) + 4)

        max_j = int(math.ceil(float(d) / 16))

        num_pad = (-t - b - 1) % 16
        if num_pad < 0:
            num_pad += 16

        len_p = BLOCK_SIZE

        P = bytearray(BLOCK_SIZE)

        P[0] = 0x01
        P[1] = 0x02
        P[2] = 0x01
        P[3] = 0x00

        tmp = struct.pack('>H', self.radix)
        P[4:6] = tmp[0:2]
        P[6] = 0x0a
        P[7] = u
        tmp = struct.pack('>L', n)
        P[8:12] = tmp[0:4]
        tmp = struct.pack('>L', t)
        P[12:len_p] = tmp[0:4]

        len_q = t + b + 1 + num_pad
        len_pq = len_p + len_q

        total_buf_len = len_q + len_pq + (max_j - 1) * BLOCK_SIZE
        buf = bytearray(total_buf_len)

        Q = memoryview(buf[:len_q])
        Q[:t] = tweak

        PQ = memoryview(buf[len_q: len_q + len_pq])

        num_radix = self.radix

        Y = memoryview(buf[len_q + len_pq - BLOCK_SIZE:])
        R = memoryview(Y[:BLOCK_SIZE])
        xored = memoryview(Y[BLOCK_SIZE:])

        num_u = u
        num_v = v
        num_mod_u = num_radix ** num_u
        num_mod_v = num_radix ** num_v
        try:
            num_a = self._decode_func(A.tobytes())
        except ValueError:
            raise CipherError(f"String not in radix {self.radix}")
        try:
            num_b = self._decode_func(B.tobytes())
        except ValueError:
            raise CipherError(f"String not in radix {self.radix}")

        for i in range(0, NUM_ROUNDS):
            Q[t + num_pad] = i
            num_b_bytes = self._get_trimmed_bytes(num_b)

            for j in range(t + num_pad + 1, len_q):
                Q[j] = 0x00

            Q[len_q - len(num_b_bytes):] = num_b_bytes
            PQ[:BLOCK_SIZE] = P
            PQ[BLOCK_SIZE:] = Q

            R[:] = self.ciph_tail(PQ)
            for j in range(1, max_j):
                offset = (j - 1) * BLOCK_SIZE
                for x in range(0, HALF_BLOCK_SIZE):
                    xored[offset + x] = 0x00

                xored[offset + HALF_BLOCK_SIZE:offset + BLOCK_SIZE] = struct.pack('>Q', j)

                for x in range(0, BLOCK_SIZE):
                    xored[offset + x] = R[x] ^ xored[offset + x]

                self.ciph(xored[offset: offset + BLOCK_SIZE])

            numY = int.from_bytes(Y[:d], byteorder='big', signed=False)
            num_c = num_a + numY
            if i % 2 == 0:
                num_c = num_c % num_mod_u
            else:
                num_c = num_c % num_mod_v

            num_a = int.from_bytes(num_b_bytes, byteorder='big', signed=False)
            num_b = num_c
        A = self._encode_func(num_a)
        B = self._encode_func(num_b)
        A = "0" * (u - len(A)) + A
        B = "0" * (v - len(B)) + B
        ret = A + B
        if radix_override:
            self._update_radix(old_radix)
        return ret.encode()

    def decrypt(self, X: bytes, radix_override: int = None):
        return self.decrypt_with_tweak(X, self.tweak, radix_override)

    def decrypt_with_tweak(self, X: bytes, tweak: bytes, radix_override) -> bytes:
        old_radix = self.radix
        if radix_override:
            self._update_radix(radix_override)
        n = len(X)
        t = len(tweak)

        if n < self.minLen or n >= self.maxLen:
            raise CipherError("message length is not within min and max bounds")

        if len(tweak) > self.maxTLen:
            raise CipherError(f"Tweak size must be no greater than {self.maxTLen} bytes in length")

        try:
            self._decode_func(X)
        except ValueError:
            raise CipherError(f"Input string {X} invalid for radix {self.radix}")

        # Calculate split point
        u = int(n / 2)
        v = n - u

        # / Split the message
        A = memoryview(X[:u])
        B = memoryview(X[u:])

        b = int(math.ceil(math.ceil(float(v) * math.log2(float(self.radix))) / 8))
        d = int(4 * math.ceil(float(b) / 4) + 4)

        max_j = int(math.ceil(float(d) / 16))

        num_pad = (-t - b - 1) % 16
        if num_pad < 0:
            num_pad += 16

        len_p = BLOCK_SIZE

        P = bytearray(BLOCK_SIZE)

        P[0] = 0x01
        P[1] = 0x02
        P[2] = 0x01
        P[3] = 0x00

        tmp = struct.pack('>H', self.radix)
        P[4:6] = tmp[0:2]
        P[6] = 0x0a
        P[7] = u
        tmp = struct.pack('>L', n)
        P[8:12] = tmp[0:4]
        tmp = struct.pack('>L', t)
        P[12:len_p] = tmp[0:4]

        len_q = t + b + 1 + num_pad
        len_pq = len_p + len_q

        total_buf_len = len_q + len_pq + (max_j - 1) * BLOCK_SIZE
        buf = bytearray(total_buf_len)

        Q = memoryview(buf[:len_q])
        Q[:t] = tweak

        PQ = memoryview(buf[len_q: len_q + len_pq])

        num_radix = self.radix

        Y = memoryview(buf[len_q + len_pq - BLOCK_SIZE:])
        R = memoryview(Y[:BLOCK_SIZE])
        xored = memoryview(Y[BLOCK_SIZE:])

        num_u = u
        num_v = v
        num_mod_u = num_radix ** num_u
        num_mod_v = num_radix ** num_v

        try:
            num_a = self._decode_func(A.tobytes())
        except ValueError:
            raise CipherError(f"String not in radix {self.radix}")
        try:
            num_b = self._decode_func(B.tobytes())
        except ValueError:
            raise CipherError(f"String not in radix {self.radix}")

        for i in range(NUM_ROUNDS - 1, -1, -1):
            Q[t + num_pad] = i
            num_a_bytes = self._get_trimmed_bytes(num_a)

            for j in range(t + num_pad + 1, len_q):
                Q[j] = 0x00

            Q[len_q - len(num_a_bytes):] = num_a_bytes
            PQ[:BLOCK_SIZE] = P
            PQ[BLOCK_SIZE:] = Q

            R[:] = self.ciph_tail(PQ)
            for j in range(1, max_j):
                offset = (j - 1) * BLOCK_SIZE
                for x in range(0, HALF_BLOCK_SIZE):
                    xored[offset + x] = 0x00

                xored[offset + HALF_BLOCK_SIZE:offset + BLOCK_SIZE] = struct.pack('>Q', j)

                for x in range(0, BLOCK_SIZE):
                    xored[offset + x] = R[x] ^ xored[offset + x]

                self.ciph(xored[offset: offset + BLOCK_SIZE])
            numY = int.from_bytes(Y[:d], byteorder='big', signed=False)
            num_c = num_b - numY

            if i % 2 == 0:
                num_c = num_c % num_mod_u
            else:
                num_c = num_c % num_mod_v

            num_b = int.from_bytes(num_a_bytes, byteorder='big', signed=False)
            num_a = num_c

        A = self._encode_func(num_a)
        B = self._encode_func(num_b)
        A = "0" * (u - len(A)) + A
        B = "0" * (v - len(B)) + B
        ret = A + B
        if radix_override:
            self._update_radix(old_radix)
        return ret.encode()

    def ciph(self, src: memoryview) -> (memoryview, CipherError):
        if len(src) % BLOCK_SIZE != 0:
            raise CipherError("length of ciph input must be multiple of 16")
        self.aes_block.encrypt_blocks(src, src)
        return src

    def ciph_tail(self, src: memoryview):
        cipher = self.ciph(src)
        return cipher[len(cipher) - BLOCK_SIZE:]
