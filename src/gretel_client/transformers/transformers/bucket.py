from dataclasses import dataclass
from numbers import Number
from typing import Union, List, Optional, Tuple

from gretel_client.transformers.base import TransformerConfig, Transformer


class BucketError(Exception):
    pass


class BucketRange:
    def __init__(self, buckets: List[Tuple], *, labels: List[str] = None, min_label='MIN_BUCKET',
                 max_label='MAX_BUCKET',
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
        self.min_bucket = min_label
        self.max_bucket = max_label


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

    def _transform_field(self, field_name: str, field_value: Union[Number, str], field_meta):
        try:
            return {field_name: mutate(field_value, self.bucket_range)}
        except (BucketError, TypeError):
            # return the actual field if an error happened
            # most likely value not being a number
            return {field_name: field_value}

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        try:
            return None, mutate(value, self.bucket_range)
        except BucketError:
            return label, value


def mutate(value: Union[str, Number], bucket_range):
    buckets = bucket_range.buckets
    if isinstance(value, str):  # We cannot reliably compare strings of different lengths, better to make them same
        value = value[:len(buckets[0][0])]
    if value < buckets[0][0]:
        return bucket_range.min_bucket
    elif value > buckets[-1][1]:
        return bucket_range.max_bucket
    for num, b_range in enumerate(buckets, start=0):
        if b_range[0] <= value <= b_range[1]:
            return bucket_range.labels[num]
    else:
        raise ValueError
