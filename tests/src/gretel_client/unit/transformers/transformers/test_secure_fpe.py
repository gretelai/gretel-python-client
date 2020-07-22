from cProfile import Profile
from pstats import Stats

from gretel_client.transformers.fpe.crypto_aes import Mode
from gretel_client.transformers.fpe.fpe_ff1 import FpeFf1
from gretel_client.transformers.transformers.fpe_base import _cipher_ff1_fpe


# Added profiling code. Uncomment the last line of each test if you like to profile FPE calls.
def test_secure_fpe_ecb():
    profiler = Profile()
    profiler.enable()
    cipher = FpeFf1(
        radix=10,
        maxTLen=0,
        key=bytearray.fromhex(
            "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94"
        ),
        tweak=b"",
        mode=Mode.ECB,
    )

    testval = [
        0.0,
        -0.0,
        89884656743115.1,
        123.4567,
        1.1,
        10.1,
        100.1,
        1000.1,
        10000.1,
        100000.1,
        1000000.1,
        2 ** 1023,
    ]
    for test_float in testval:
        for prec in range(0, 10):
            encode_t = _cipher_ff1_fpe((test_float, prec), FpeFf1.encrypt, cipher)
            decode_t = _cipher_ff1_fpe((encode_t, prec), FpeFf1.decrypt, cipher)
            assert test_float == decode_t
            assert test_float - encode_t < (1 / (10 ** prec))

    test_float = -987651.9432
    while test_float < -0.1:
        encode_t = _cipher_ff1_fpe(test_float, FpeFf1.encrypt, cipher)
        decode_t = _cipher_ff1_fpe(encode_t, FpeFf1.decrypt, cipher)
        assert test_float == decode_t
        test_float += abs(test_float * 0.01)

    test_float = 987651.9432
    while test_float > 0.1:
        encode_t = _cipher_ff1_fpe(test_float, FpeFf1.encrypt, cipher)
        decode_t = _cipher_ff1_fpe(encode_t, FpeFf1.decrypt, cipher)
        assert test_float == decode_t
        test_float -= abs(test_float * 0.01)

    stats = Stats(profiler)
    stats.strip_dirs()
    stats.sort_stats("cumtime")
    # stats.print_stats()


def test_secure_fpe_cbc():
    profiler = Profile()
    profiler.enable()
    cipher = FpeFf1(
        radix=10,
        maxTLen=0,
        key=bytearray.fromhex(
            "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94"
        ),
        tweak=b"",
        mode=Mode.CBC,
    )

    testval = [
        0.0,
        -0.0,
        89884656743115.1,
        123.4567,
        1.1,
        10.1,
        100.1,
        1000.1,
        10000.1,
        100000.1,
        1000000.1,
        2 ** 1023,
    ]
    for test_float in testval:
        for prec in range(0, 10):
            encode_t = _cipher_ff1_fpe((test_float, prec), FpeFf1.encrypt, cipher)
            decode_t = _cipher_ff1_fpe((encode_t, prec), FpeFf1.decrypt, cipher)
            assert test_float == decode_t
            assert test_float - encode_t < (1 / (10 ** prec))

    test_float = -987651.9432
    while test_float < -0.1:
        encode_t = _cipher_ff1_fpe(test_float, FpeFf1.encrypt, cipher)
        decode_t = _cipher_ff1_fpe(encode_t, FpeFf1.decrypt, cipher)
        assert test_float == decode_t
        test_float += abs(test_float * 0.01)

    test_float = 987651.9432
    while test_float > 0.1:
        encode_t = _cipher_ff1_fpe(test_float, FpeFf1.encrypt, cipher)
        decode_t = _cipher_ff1_fpe(encode_t, FpeFf1.decrypt, cipher)
        assert test_float == decode_t
        test_float -= abs(test_float * 0.01)

    stats = Stats(profiler)
    stats.strip_dirs()
    stats.sort_stats("cumtime")
    # stats.print_stats()


def test_secure_fpe_cbc_fast():
    profiler = Profile()
    profiler.enable()
    cipher = FpeFf1(
        radix=10,
        maxTLen=0,
        key=bytearray.fromhex(
            "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94"
        ),
        tweak=b"",
        mode=Mode.CBC_FAST,
    )

    testval = [
        0.0,
        -0.0,
        89884656743115.1,
        123.4567,
        1.1,
        10.1,
        100.1,
        1000.1,
        10000.1,
        100000.1,
        1000000.1,
        2 ** 1023,
    ]
    for test_float in testval:
        for prec in range(0, 10):
            encode_t = _cipher_ff1_fpe((test_float, prec), FpeFf1.encrypt, cipher)
            decode_t = _cipher_ff1_fpe((encode_t, prec), FpeFf1.decrypt, cipher)
            assert test_float == decode_t
            assert test_float - encode_t < (1 / (10 ** prec))

    test_float = -987651.9432
    while test_float < -0.1:
        encode_t = _cipher_ff1_fpe(test_float, FpeFf1.encrypt, cipher)
        decode_t = _cipher_ff1_fpe(encode_t, FpeFf1.decrypt, cipher)
        assert test_float == decode_t
        test_float += abs(test_float * 0.01)

    test_float = 987651.9432
    while test_float > 0.1:
        encode_t = _cipher_ff1_fpe(test_float, FpeFf1.encrypt, cipher)
        decode_t = _cipher_ff1_fpe(encode_t, FpeFf1.decrypt, cipher)
        assert test_float == decode_t
        test_float -= abs(test_float * 0.01)

    stats = Stats(profiler)
    stats.strip_dirs()
    stats.sort_stats("cumtime")
    # stats.print_stats()
