from dataclasses import dataclass
from numbers import Number
from typing import Union, List, Tuple

from gretel_client.transformers.base import TransformerConfig, Transformer


class BucketError(Exception):
    pass


class BucketRange:
    def __init__(self, buckets: List[Tuple], *, labels: Union[List[Number], List[str]] = None, min_label=None,
                 max_label=None,
                 auto_label_prefix='BUCKET_'):
        # buckets should be all strings or all numbers
        str_check = [isinstance(item[0], str) and isinstance(item[1], str) for item in buckets]
        num_check = [isinstance(item[0], Number) and isinstance(item[1], Number) for item in buckets]
        if not all(str_check) and not all(num_check):
            raise BucketError('buckets must be all strings or all numbers')
        self.buckets = buckets
        if not labels:
            labels = [auto_label_prefix + str(x) for x in range(len(buckets))]
        self.labels = labels
        self.min_bucket = min_label if min_label is not None else labels[0]
        self.max_bucket = max_label if max_label is not None else labels[-1]


@dataclass(frozen=True)
class BucketConfig(TransformerConfig):
    bucket_range: BucketRange = None


class Bucket(Transformer):
    """
    Bucket transformer replaces a field value with the label in the given value range.
    """
    config_class = BucketConfig

    def __init__(self, config: BucketConfig):
        super().__init__(config)
        self.bucket_range = config.bucket_range

    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        buckets = self.bucket_range.buckets
        if isinstance(value, str):  # We cannot reliably compare strings of different lengths, better to make them same
            value = value[:len(buckets[0][0])]
        if value < buckets[0][0]:
            return self.bucket_range.min_bucket
        elif value > buckets[-1][1]:
            return self.bucket_range.max_bucket
        for num, b_range in enumerate(buckets, start=0):
            if b_range[0] <= value <= b_range[1]:
                return self.bucket_range.labels[num]
        else:
            raise ValueError
