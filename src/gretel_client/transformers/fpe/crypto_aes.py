from enum import Enum

from Crypto.Cipher import AES

BLOCK_SIZE = AES.block_size
IV_ZERO = memoryview(bytes(BLOCK_SIZE))


class CipherError(Exception):
    pass


def xor_bytes(dst: memoryview, a: memoryview, b: memoryview):
    dst[:] = bytes(at ^ bt for (at, bt) in zip(a, b))


def xor_bytes_val(dst: memoryview, a: memoryview, val: int):
    dst[:] = bytes(at ^ val for at in a)


class Mode(Enum):
    CBC_FAST = 0
    ECB = 1
    CBC = 2


class AESCipher:

    def __init__(self, key: bytes, mode=Mode.CBC_FAST):
        self.mode = mode
        self.key = key
        self._cipher = None
        self._decipher = None
        self._cipher_iv = IV_ZERO
        self._decipher_iv = IV_ZERO
        if mode is Mode.ECB:
            self.encrypt_blocks = self.encrypt_blocks_ecb
            self.decrypt_blocks = self.decrypt_blocks_ecb
            self.AES_kwargs = {'key': self.key, 'mode': AES.MODE_ECB}
        elif mode is Mode.CBC_FAST:
            self.encrypt_blocks = self.encrypt_blocks_cbc_iv_zero_fast
            self.decrypt_blocks = self.decrypt_blocks_cbc_iv_zero_fast
            self.AES_kwargs = {'key': self.key, 'mode': AES.MODE_CBC, 'iv': IV_ZERO}
        elif mode is Mode.CBC:
            self.encrypt_blocks = self.encrypt_blocks_cbc_iv_zero
            self.decrypt_blocks = self.decrypt_blocks_cbc_iv_zero
            self.AES_kwargs = {'key': self.key, 'mode': AES.MODE_CBC, 'iv': IV_ZERO}
        else:
            raise ValueError

    def reset_cipher(self):
        self._cipher = None
        self._decipher = None
        self._cipher_iv = IV_ZERO
        self._decipher_iv = IV_ZERO

    def decrypt(self, dst: memoryview, src: memoryview):
        if not self._decipher:
            self._decipher = AES.new(**self.AES_kwargs)
        dst[:] = self._decipher.decrypt(src.tobytes())

    def encrypt(self, dst: memoryview, src: memoryview):
        if not self._cipher:
            self._cipher = AES.new(**self.AES_kwargs)
        dst[:] = self._cipher.encrypt(src.tobytes())

    def decrypt_blocks_ecb(self, dst: memoryview, src: memoryview):
        if len(src) % BLOCK_SIZE != 0:
            raise CipherError("crypto/cipher: input not full blocks")
        if len(dst) < len(src):
            raise CipherError("crypto/cipher: output smaller than input")
        self.decrypt(dst, src)

    def encrypt_blocks_ecb(self, dst: memoryview, src: memoryview):
        if len(src) % BLOCK_SIZE != 0:
            raise CipherError("crypto/cipher: input not full blocks")
        if len(dst) < len(src):
            raise CipherError("crypto/cipher: output smaller than input")
        self.encrypt(dst, src)

    def encrypt_blocks_cbc_iv_zero_fast(self, dst: memoryview, src: memoryview):
        if len(src) % BLOCK_SIZE != 0:
            raise CipherError("crypto/cipher: input not full blocks")
        if len(dst) < len(src):
            raise CipherError("crypto/cipher: output smaller than input")

        while len(src) > 0:
            xor_bytes(dst[:BLOCK_SIZE], src[:BLOCK_SIZE], self._cipher_iv)
            self.encrypt(dst[:BLOCK_SIZE], dst[:BLOCK_SIZE])

            self._cipher_iv = dst[:BLOCK_SIZE]
            src = src[BLOCK_SIZE:]
            dst = dst[BLOCK_SIZE:]

    def decrypt_blocks_cbc_iv_zero_fast(self, dst: memoryview, src: memoryview):
        if len(src) % BLOCK_SIZE != 0:
            raise CipherError("crypto/cipher: input not full blocks")
        if len(dst) < len(src):
            raise CipherError("crypto/cipher: output smaller than input")

        while len(src) > 0:
            self.decrypt(dst[:BLOCK_SIZE], src[:BLOCK_SIZE])
            xor_bytes(dst[:BLOCK_SIZE], dst[:BLOCK_SIZE], self._decipher_iv)
            self._decipher_iv = src[:BLOCK_SIZE]

            src = src[BLOCK_SIZE:]
            dst = dst[BLOCK_SIZE:]

    def encrypt_blocks_cbc_iv_zero(self, dst: memoryview, src: memoryview):
        if len(src) % BLOCK_SIZE != 0:
            raise CipherError("crypto/cipher: input not full blocks")
        if len(dst) < len(src):
            raise CipherError("crypto/cipher: output smaller than input")

        xor_bytes_val(dst, src, 0)
        self.encrypt(dst, dst)
        self.reset_cipher()

    def decrypt_blocks_cbc_iv_zero(self, dst: memoryview, src: memoryview):
        if len(src) % BLOCK_SIZE != 0:
            raise CipherError("crypto/cipher: input not full blocks")
        if len(dst) < len(src):
            raise CipherError("crypto/cipher: output smaller than input")

        self.decrypt(dst, src)
        xor_bytes_val(dst, dst, 0)
        self.reset_cipher()
