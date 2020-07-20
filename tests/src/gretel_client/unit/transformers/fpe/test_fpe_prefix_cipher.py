from gretel_client.transformers.fpe.fpe_prefix_cipher import FpePrefixCipher

KEY1 = bytearray.fromhex(
    "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94"
)
KEY2 = bytearray.fromhex(
    "1628AED2A6A809CBF7158F4F3CEF4380AA4F7F036D6F059D8D54FC6A942B7E15"
)


def test_fpe_prefix_cipher():
    cipher1 = FpePrefixCipher(-10, 20, KEY1)
    cipher2 = FpePrefixCipher(-10, 20, KEY2)
    cipher_list1 = []
    cipher_list2 = []
    for i in range(-10, 20):
        cipher_list1.append(cipher1.encrypt(i))
    for i in range(-10, 20):
        cipher_list2.append(cipher2.encrypt(i))

    decipher_list1 = []
    decipher_list2 = []
    for i in cipher_list1:
        decipher_list1.append(cipher1.decrypt(i))
    for i in cipher_list2:
        decipher_list2.append(cipher2.decrypt(i))
    assert cipher_list1 != cipher_list2
    assert cipher_list1 != decipher_list2
    assert cipher_list1 == [
        13,
        -5,
        17,
        3,
        -8,
        -2,
        12,
        15,
        -1,
        8,
        5,
        7,
        -3,
        -6,
        1,
        4,
        16,
        14,
        18,
        10,
        0,
        11,
        -10,
        2,
        -9,
        -4,
        -7,
        6,
        19,
        9,
    ]
    assert cipher_list2 == [
        8,
        5,
        -2,
        1,
        -6,
        19,
        18,
        -9,
        6,
        -7,
        10,
        16,
        9,
        2,
        15,
        3,
        11,
        4,
        -8,
        -4,
        0,
        -5,
        -10,
        7,
        14,
        12,
        13,
        17,
        -1,
        -3,
    ]
    assert decipher_list2 == [i for i in range(-10, 20)]
