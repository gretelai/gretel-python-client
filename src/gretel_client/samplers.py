from collections.abc import Iterator
import random


def get_default_sampler():
    return ConstantSampler()


class Sampler(Iterator):
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:  # pragma: no cover
        return self._name

    def set_source(self, data_source):
        self.data_source = data_source

    def __iter__(self) -> Iterator:  # pragma: no cover
        return self

    def __next__(self):  # pragma: no cover
        return next(self.data_source)


class ConstantSampler(Sampler):
    """Iterator generator that returns a sampled record based
    on a naive probabilistic sampling.

    Args:
        sample_rate: determines the rate to sample records. For example
            if sample_rate=4, 1 in 4 records will be written. If
            sample_rate=10, 1 in 10 records will be written.
        record_limit: the max number of records to write. If the number
            is < 0 all records will be written.
    """
    def __init__(self, sample_rate: int = 1, record_limit: int = -1):
        self.sample_rate = sample_rate
        self.record_limit = record_limit
        self.records_seen = 0
        super().__init__("constant_sampler")

    def __iter__(self):
        return self

    def __next__(self):
        if self.record_limit > 0 and self.records_seen >= self.record_limit:
            raise StopIteration(f"Reach record limit of {self.record_limit}.")

        while random.random() > 1.0 / self.sample_rate:
            self.records_seen += 1
            next(self.data_source)

        self.records_seen += 1
        return next(self.data_source)


class FixedSample(Sampler):
    def __init__(
        self,
        percent: float = None,
        count: int = None,
        min_count_threshold: int = None
    ):
        if percent and count:
            raise AttributeError("Cannot specify percent and count args. Pick one.")
        self.percent = percent
        self.count = count
        self.min_count_threshold = min_count_threshold
        self.record_idx = 0
        self.rate = 1
        super().__init__("fixed_sample")

    def __iter__(self):
        self.records = [r for r in self.data_source]
        self.record_it = iter(self.records)
        self.record_count = len(self.records)

        if self.min_count_threshold and self.record_count < self.min_count_threshold:
            return self

        if self.count:
            self.rate = int(self.record_count / self.count)

        if self.percent:
            self.rate = int(self.record_count / (self.record_count * self.percent))

        return self

    def __next__(self):

        while self.record_idx % self.rate != 0:
            self.record_idx += 1
            next(self.record_it)

        self.record_idx += 1
        return next(self.record_it)
