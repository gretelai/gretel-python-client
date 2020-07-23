from dataclasses import dataclass
from numbers import Number
from typing import Union, List, Tuple

from gretel_client.transformers.base import TransformerConfig, Transformer


@dataclass(frozen=True)
class BucketConfig(TransformerConfig):
    """
    Sort numeric data into buckets.  Each bucket has a numeric or string label.

    Args:
        buckets: Tuple of three floats or ints to specify minimum, maximum and bucket width.
            List of int or float to explicitly specify bucket boundaries.  Bucket boundaries are left inclusive.
        lower_outlier_label: Float, int or string.  This label will be applied to values greater or equal to the
            maximum bucketed value.  If None, use the first bucket label.
        upper_outlier_label: Float, int or string.  This label will be applied to values less than the
            minimum bucketed value.  If None, use the last bucket label.
    """

    buckets: List[
        Tuple[Union[Number, str], Union[Number, str], Union[Number, str]]
    ] = None
    lower_outlier_label: Union[Number, str] = None
    upper_outlier_label: Union[Number, str] = None


def bucket_tuple_to_list(
    buckets: Tuple[Number, Number, Number] = None,
    labels: List[Union[Number, str]] = None,
    label_method: str = None,
) -> List[Tuple[Union[Number, str], Union[Number, str], Union[Number, str]]]:
    """
    Helper function.  Convert a min/max/width tuple used by BucketConfig into an explicit list of values.  Use it,
    for example, to start with a standard list and then modify it to have varying width buckets.

    Args:
        buckets: Tuple of three floats or ints to specify minimum, maximum and bucket width.
        labels: (Optional) List of labels, must match length of resulting bucket list.
        label_method: (Optional) if labels is None, one of 'min', 'max' or 'avg' can be specified, so that each
            bucket uses either the left or right endpoint or the average of the two as the bucket label. Default: "min"
    Returns:
        Explicit list of bucket boundaries.
    """
    if buckets is None:
        raise ValueError("buckets must be a 3-Tuple (min, max, width)")
    if labels is None:
        labels = get_bucket_labels_from_tuple(buckets, label_method)
    b = []
    current_min = buckets[0]
    idx = 0
    while current_min < buckets[1]:
        current_max = (
            current_min + buckets[2]
            if current_min + buckets[2] < buckets[1]
            else buckets[1]
        )
        b.append((current_min, current_max, None if not labels else labels[idx]))
        current_min += buckets[2]
        idx += 1
    return b


def get_bucket_labels_from_tuple(
    bucket_creation_params: Tuple[Number, Number, Number] = None,
    label_method: str = None,
) -> List[Number]:
    """
    Helper function.  Convert a min/max/width tuple used by BucketConfig into a list of bucket labels.  The
    labels can be the minimum, average or maximum value for each bucket.

    Args:
        bucket_creation_params: Tuple of three floats or ints to specify minimum, maximum and bucket width.
        label_method: One of 'min', 'max' or 'avg'.  For each bucket, use either the left or right endpoint or the
            average of the two as the bucket label. Default: "min"

    Returns:
        List of numeric bucket labels.
    """
    if bucket_creation_params is None:
        raise ValueError("buckets must be a 3-Tuple (min, max, width)")
    # Use minimum by default
    label_method = label_method or "min"
    if label_method not in ["min", "max", "avg"]:
        raise ValueError(
            f"label_method must be one of 'min', 'max', 'avg': {label_method}"
        )
    bucket_labels = []
    current_min = bucket_creation_params[0]
    while current_min < bucket_creation_params[1]:
        if label_method == "min":
            bucket_labels.append(current_min)
        elif label_method == "max":
            bucket_labels.append(
                min(current_min + bucket_creation_params[2], bucket_creation_params[1])
            )
        elif label_method == "avg":
            bucket_labels.append(current_min + (bucket_creation_params[2] / 2))
        current_min += bucket_creation_params[2]
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
        self.buckets = config.buckets
        if self.buckets is None or len(self.buckets) == 0:
            raise ValueError("Empty buckets not permitted.")
        self.lower_outlier_label = (
            self.buckets[0][2]
            if config.lower_outlier_label is None
            else config.lower_outlier_label
        )
        self.upper_outlier_label = (
            self.buckets[-1][2]
            if config.upper_outlier_label is None
            else config.upper_outlier_label
        )

    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        try:
            return self._mutate_num(value)
        except (TypeError, ValueError):
            return value

    def _mutate_num(self, value: Union[Number, str]) -> Union[Number, str]:
        if value < self.buckets[0][0]:
            return self.lower_outlier_label
        elif value >= self.buckets[-1][1]:
            return self.upper_outlier_label
        for idx in range(len(self.buckets)):
            if self.buckets[idx][0] <= value < self.buckets[idx][1]:
                return self.buckets[idx][2]
