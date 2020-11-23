from typing import Iterator
import pytest

from gretel_client.samplers import FixedSample


def data_source(count) -> Iterator[int]:
    for n in range(count):
        yield n

def test_fixed_count_sample():
    sampler = FixedSample(count=10)
    sampler.set_source(data_source(1000))
    sampled = [r for r in sampler]
    assert sampler.rate == 100
    assert sampled == [0, 100, 200, 300, 400, 500, 600, 700, 800, 900]


def test_fixed_percent():
    sampler = FixedSample(percent=.2)
    sampler.set_source(data_source(1000))
    sampled = [r for r in sampler]

    assert sampler.rate == 5
    assert len(sampled) == 200
    assert sampled[0] == 0 and sampled[-1] == 995


def test_fixed_min_threshold():
    sampler = FixedSample(count=100, min_count_threshold=5000)
    sampler.set_source(data_source(1000))
    sampled = [r for r in sampler]
    assert sampler.rate == 1
    assert len(sampled) == 1000

