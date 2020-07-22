from dataclasses import dataclass
from typing import Union, List, Optional, Tuple

from gretel_client.transformers.base import TransformerConfig, Transformer


@dataclass(frozen=True)
class BucketConfig(TransformerConfig):
    """
    Sort numeric data into buckets.  Each bucket has a numeric or string label.

    Args:
        buckets: Tuple of three floats or ints to specify minimum, maximum and bucket width.
            List of int or float to explicitly specify bucket boundaries.  Bucket boundaries are left inclusive.
        bucket_labels: List of floats, ints or strings.  The label to attach to each bucket.  The length of this
            list must match the number of buckets, i.e. one less than the length of a list passed to the
            buckets argument. The field value will be changed to the corresponding label after bucketing.
        lower_outlier_label: Float, int or string.  This label will be applied to values greater or equal to the
            maximum bucketed value.  If None, use the first bucket label.
        upper_outlier_label: Float, int or string.  This label will be applied to values less than the
            minimum bucketed value.  If None, use the last bucket label.
    """
    buckets: Union[List[float], List[int], Tuple[Union[float, int], Union[float, int], Union[float, int]]] = None
    bucket_labels: Union[List[float], List[int], List[str]] = None
    lower_outlier_label: Union[float, int, str] = None
    upper_outlier_label: Union[float, int, str] = None


def bucket_tuple_to_list(
        buckets: Tuple[Union[float, int], Union[float, int], Union[float, int]] = None) \
        -> Union[List[float], List[int]]:
    """
    Helper function.  Convert a min/max/width tuple used by BucketConfig into an explicit list of values.  Use it,
    for example, to start with a standard list and then modify it to have varying width buckets.

    Args:
        buckets: Tuple of three floats or ints to specify minimum, maximum and bucket width.

    Returns:
        Explicit list of bucket boundaries.
    """
    if buckets is None:
        raise ValueError(f"buckets must be a 3-Tuple (min, max, width)")
    b = []
    current_min = buckets[0]
    while current_min < buckets[1]:
        b.append(current_min)
        current_min += buckets[2]
    b.append(buckets[1])
    return b


def get_bucket_labels_from_tuple(
        buckets: Tuple[Union[float, int], Union[float, int], Union[float, int]] = None,
        method: str = None) \
        -> Union[List[float], List[int]]:
    """
    Helper function.  Convert a min/max/width tuple used by BucketConfig into a list of bucket labels.  The
    labels can be the minimum, average or maximum value for each bucket.

    Args:
        buckets: Tuple of three floats or ints to specify minimum, maximum and bucket width.
        method: One of 'min', 'max' or 'avg'.  For each bucket, use either the left or right endpoint or the
            average of the two as the bucket label.

    Returns:
        List of numeric bucket labels.
    """
    if buckets is None:
        raise ValueError(f"buckets must be a 3-Tuple (min, max, width)")
    # Use average by default
    method = method or "avg"
    if method not in ["min", "max", "avg"]:
        raise ValueError(f"method must be one of 'min', 'max', 'avg': {method}")
    bucket_labels = []
    current_min = buckets[0]
    while current_min < buckets[1]:
        if method == "min":
            bucket_labels.append(current_min)
        elif method == "max":
            bucket_labels.append(min(current_min + buckets[2], buckets[1]))
        elif method == "avg":
            bucket_labels.append(current_min + (buckets[2] / 2))
        current_min += buckets[2]
    return bucket_labels


class Bucket(Transformer):
    """
    Bucket transformer.  Sort numeric fields into buckets.  The field value is changed into the numeric or string
    label for that bucket.  Extra labels can be specified for values falling outside of the bucket range.
    """
    config_class = BucketConfig

    def __init__(self, config: BucketConfig):
        super().__init__(config)
        # Expand tuple to list
        if type(config.buckets) == tuple:
            self.buckets = bucket_tuple_to_list(config.buckets)
        else:
            self.buckets = config.buckets
        self.bucket_labels = config.bucket_labels
        # Sanity check parameters
        if self.buckets is None or len(self.buckets) == 0:
            raise ValueError(f"Empty buckets not permitted.")
        if len(self.buckets) - 1 != len(self.bucket_labels):
            raise ValueError(f"Number of bucket_labels must match number of buckets.")
        self.lower_outlier_label = self.bucket_labels[0] if config.lower_outlier_label is None \
            else config.lower_outlier_label
        self.upper_outlier_label = self.bucket_labels[-1] if config.upper_outlier_label is None \
            else config.upper_outlier_label

    def _transform_field(self, field_name: str, field_value: Union[float, int], field_meta):
        try:
            return {field_name: self._mutate(field_value)}
        except (TypeError, ValueError):
            # Return the original field if an error based on bad input happened
            return {field_name: field_value}

    def _transform_entity(
            self, label: str, value: Union[float, int]) -> Optional[Tuple[Optional[str], Union[float, int, str]]]:
        try:
            return None, self._mutate(value)
        except (TypeError, ValueError):
            # Return the original field if an error based on bad input happened
            return None, value

    def _mutate(self, value: Union[float, int]) -> Union[float, int, str]:
        if value < self.buckets[0]:
            return self.lower_outlier_label
        elif value >= self.buckets[-1]:
            return self.upper_outlier_label
        for idx in range(len(self.buckets) - 1):
            if self.buckets[idx] <= value < self.buckets[idx + 1]:
                return self.bucket_labels[idx]
