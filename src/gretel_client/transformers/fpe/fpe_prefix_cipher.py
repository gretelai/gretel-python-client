from Crypto.Cipher import AES
from gretel_client.transformers.fpe.crypto_aes import IV_ZERO, CipherError


class FpePrefixCipher:
    def __init__(self, min: int, max: int, key: bytes):
        self.min = min
        self.max = max
        self.range = max - min
        weights = []
        cipher = AES.new(key, AES.MODE_CBC, iv=IV_ZERO)
        for i in range(0, self.range):
            cipher_val = int.from_bytes(cipher.encrypt(key), byteorder='big', signed=False)
            weights.append([cipher_val, i])
        weights.sort()
        self._encrypt = [x[1] for x in weights]
        # TODO: clean the for loop up
        i = 0
        for x in weights:
            x[0] = x[1]
            x[1] = i
            i += 1
        weights.sort()

        self._decrypt = [x[1] for x in weights]

    def encrypt(self, value: int) -> int:
        if value < self.min or value > self.max:
            raise CipherError(f"input value out of range ({self.min}...{self.max})")
        value -= self.min
        return self._encrypt[value] + self.min

    def decrypt(self, value: int) -> int:
        if value < self.min or value > self.max:
            raise CipherError(f"input value out of range ({self.min}...{self.max})")
        value -= self.min
        return self._decrypt[value] + self.min
