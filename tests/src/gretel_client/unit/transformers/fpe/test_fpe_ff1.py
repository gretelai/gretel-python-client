from Crypto.Cipher import AES

from gretel_client.transformers.fpe.crypto_aes import Mode
from gretel_client.transformers.fpe.fpe_ff1 import FpeFf1

# Official NIST FF1 Test Vectors
# Key and tweak are both hex-encoded strings
# 'radix'      :int
# 'key'        :string
# 'tweak'      :string
# 'plaintext'  :string
# 'ciphertext' :string

TEST_VECTORS_CBC = [
    {
        "cipher": "AES-128-CBC",
        "radix": 16,
        "key": "2B7E151628AED2A6ABF7158809CF4F3C",
        "tweak": "",
        "iv": "000102030405060708090A0B0C0D0E0F",
        "plaintext": "6BC1BEE22E409F96E93D7E117393172A",
        "ciphertext": "7649ABAC8119B246CEE98E9B12E9197D",
    },
    {
        "cipher": "AES-128-CBC",
        "radix": 16,
        "key": "2B7E151628AED2A6ABF7158809CF4F3C",
        "tweak": "",
        "iv": "7649ABAC8119B246CEE98E9B12E9197D",
        "plaintext": "AE2D8A571E03AC9C9EB76FAC45AF8E51",
        "ciphertext": "5086CB9B507219EE95DB113A917678B2",
    },
    {
        "cipher": "AES-128-CBC",
        "key": "2B7E151628AED2A6ABF7158809CF4F3C",
        "tweak": "",
        "iv": "5086CB9B507219EE95DB113A917678B2",
        "plaintext": "30C81C46A35CE411E5FBC1191A0A52EF",
        "ciphertext": "73BED6B8E3C1743B7116E69E22229516",
    },
    {
        "cipher": "AES-128-CBC",
        "key": "2B7E151628AED2A6ABF7158809CF4F3C",
        "tweak": "",
        "iv": "73BED6B8E3C1743B7116E69E22229516",
        "plaintext": "F69F2445DF4F9B17AD2B417BE66C3710",
        "ciphertext": "3FF1CAA1681FAC09120ECA307586E1A7",
        # CBC-AES256.Encrypt and CBC-AES256.Decrypt
    },
    {
        "cipher": "AES-256-CBC",
        "key": "603DEB1015CA71BE2B73AEF0857D77811F352C073B6108D72D9810A30914DFF4",
        "tweak": "",
        "iv": "000102030405060708090A0B0C0D0E0F",
        "plaintext": "6BC1BEE22E409F96E93D7E117393172A",
        "ciphertext": "F58C4C04D6E5F1BA779EABFB5F7BFBD6",
    },
    {
        "cipher": "AES-256-CBC",
        "key": "603DEB1015CA71BE2B73AEF0857D77811F352C073B6108D72D9810A30914DFF4",
        "tweak": "",
        "iv": "F58C4C04D6E5F1BA779EABFB5F7BFBD6",
        "plaintext": "AE2D8A571E03AC9C9EB76FAC45AF8E51",
        "ciphertext": "9CFC4E967EDB808D679F777BC6702C7D",
    },
    {
        "cipher": "AES-256-CBC",
        "key": "603DEB1015CA71BE2B73AEF0857D77811F352C073B6108D72D9810A30914DFF4",
        "tweak": "",
        "iv": "9CFC4E967EDB808D679F777BC6702C7D",
        "plaintext": "30C81C46A35CE411E5FBC1191A0A52EF",
        "ciphertext": "39F23369A9D9BACFA530E26304231461",
    },
    {
        "cipher": "AES-256-CBC",
        "key": "603DEB1015CA71BE2B73AEF0857D77811F352C073B6108D72D9810A30914DFF4",
        "tweak": "",
        "iv": "39F23369A9D9BACFA530E26304231461",
        "plaintext": "F69F2445DF4F9B17AD2B417BE66C3710",
        "ciphertext": "B2EB05E2C39BE9FCDA6C19078C6A9D1B",
    },
]

TEST_VECTORS_AES_FPE = [
    # AES-128
    {
        "radix": 10,
        "key": "2B7E151628AED2A6ABF7158809CF4F3C",
        "tweak": "",
        "plaintext": "0123456789",
        "ciphertext": "2433477484",
    },
    {
        "radix": 10,
        "key": "2B7E151628AED2A6ABF7158809CF4F3C",
        "tweak": "39383736353433323130",
        "plaintext": "0123456789",
        "ciphertext": "6124200773",
    },
    {
        "radix": 36,
        "key": "2B7E151628AED2A6ABF7158809CF4F3C",
        "tweak": "3737373770717273373737",
        "plaintext": "0123456789abcdefghi",
        "ciphertext": "a9tv40mll9kdu509eum",
    },
    # AES-192
    {
        "radix": 10,
        "key": "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F",
        "tweak": "",
        "plaintext": "0123456789",
        "ciphertext": "2830668132",
    },
    {
        "radix": 10,
        "key": "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F",
        "tweak": "39383736353433323130",
        "plaintext": "0123456789",
        "ciphertext": "2496655549",
    },
    {
        "radix": 36,
        "key": "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F",
        "tweak": "3737373770717273373737",
        "plaintext": "0123456789abcdefghi",
        "ciphertext": "xbj3kv35jrawxv32ysr",
    },
    # AES-256
    {
        "radix": 10,
        "key": "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
        "tweak": "",
        "plaintext": "0123456789",
        "ciphertext": "6657667009",
    },
    {
        "radix": 10,
        "key": "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
        "tweak": "39383736353433323130",
        "plaintext": "0123456789",
        "ciphertext": "1001623463",
    },
    {
        "radix": 36,
        "key": "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
        "tweak": "3737373770717273373737",
        "plaintext": "0123456789abcdefghi",
        "ciphertext": "xs8a0azh2avyalyzuwd",
    },
    # our own tests from here on out
    {
        "radix": 36,
        "key": "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
        "tweak": "3737373770717273373737",
        "plaintext": "0123456789abcdefghijklmnopqrstuvwxyz578154718501dhjvnhkjfsdbnvdnbsdkjbnslw",
        "ciphertext": "nd4dnyyln544fsdzc3s4k0dx9cbl73egz7c4a79ckpwxbzc3gejrq7r49z1x4kakrxatltrc2y",
    },
    {
        "radix": 10,
        "key": "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
        "tweak": "",
        "plaintext": "100000000",
        "ciphertext": "128994144",
    },
]


def test_ff1_fpe():
    for testvector in TEST_VECTORS_AES_FPE:
        cipher = FpeFf1(
            radix=testvector["radix"],
            maxTLen=len(testvector["tweak"]),
            key=bytearray.fromhex(testvector["key"]),
            tweak=bytearray.fromhex(testvector["tweak"]),
            mode=Mode.CBC,
        )
        plaintext = testvector["plaintext"].encode()
        cipher_text = cipher.encrypt(plaintext)
        decipher_text = cipher.decrypt(cipher_text)
        cipher_test = testvector["ciphertext"].encode()
        assert decipher_text == plaintext
        assert cipher_text == cipher_test


def test_aes_cbc():
    for testvector in TEST_VECTORS_CBC:
        cipher = AES.new(
            bytearray.fromhex(testvector["key"]),
            AES.MODE_CBC,
            iv=bytearray.fromhex(testvector["iv"]),
        )
        decipher = AES.new(
            bytearray.fromhex(testvector["key"]),
            AES.MODE_CBC,
            iv=bytearray.fromhex(testvector["iv"]),
        )
        plaintext = bytearray.fromhex(testvector["plaintext"])
        ciphertext = cipher.encrypt(plaintext)
        plaintest = decipher.decrypt(ciphertext)
        ciphertext = ciphertext.hex()
        plaintext = plaintext.hex()
        plaintest = plaintest.hex()
        ciphertest = testvector["ciphertext"].lower()
        assert plaintest == plaintext
        assert ciphertest == ciphertext
