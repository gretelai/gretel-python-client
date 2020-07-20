import pytest


@pytest.fixture(scope='session')
def test_vectors():
    """Official NIST FF1 Test Vectors
    Key and tweak are both hex-encoded strings
    'radix'      :int
    'key'        :string
    'tweak'      :string
    'plaintext'  :string
    'ciphertext' :string"""
    return [
        # AES-128
        {
            'radix': 10,
            'key': "2B7E151628AED2A6ABF7158809CF4F3C",
            'tweak': "",
            'plaintext': "0123456789",
            'ciphertext': "2433477484"
        },
        {
            'radix': 10,
            'key': "2B7E151628AED2A6ABF7158809CF4F3C",
            'tweak': "39383736353433323130",
            'plaintext': "0123456789",
            'ciphertext': "6124200773"
        },
        {
            'radix': 36,
            'key': "2B7E151628AED2A6ABF7158809CF4F3C",
            'tweak': "3737373770717273373737",
            'plaintext': "0123456789abcdefghi",
            'ciphertext': "a9tv40mll9kdu509eum"
        },

        # AES-192
        {
            'radix': 10,
            'key': "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F",
            'tweak': "",
            'plaintext': "0123456789",
            'ciphertext': "2830668132"
        },
        {
            'radix': 10,
            'key': "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F",
            'tweak': "39383736353433323130",
            'plaintext': "0123456789",
            'ciphertext': "2496655549"
        },
        {
            'radix': 36,
            'key': "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F",
            'tweak': "3737373770717273373737",
            'plaintext': "0123456789abcdefghi",
            'ciphertext': "xbj3kv35jrawxv32ysr"
        },

        # AES-256
        {
            'radix': 10,
            'key': "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
            'tweak': "",
            'plaintext': "0123456789",
            'ciphertext': "6657667009"
        },
        {
            'radix': 10,
            'key': "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
            'tweak': "39383736353433323130",
            'plaintext': "0123456789",
            'ciphertext': "1001623463"
        },
        {
            'radix': 36,
            'key': "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
            'tweak': "3737373770717273373737",
            'plaintext': "0123456789abcdefghi",
            'ciphertext': "xs8a0azh2avyalyzuwd"
        },
        # our own tests from here on out
        {
            'radix': 36,
            'key': "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
            'tweak': "3737373770717273373737",
            'plaintext': "0123456789abcdefghijklmnopqrstuvwxyz578154718501dhjvnhkjfsdbnvdnbsdkjbnslw",
            'ciphertext': "nd4dnyyln544fsdzc3s4k0dx9cbl73egz7c4a79ckpwxbzc3gejrq7r49z1x4kakrxatltrc2y"
        },
        {
            'radix': 10,
            'key': "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
            'tweak': "",
            'plaintext': "100000000",
            'ciphertext': "128994144"
        }
    ]
