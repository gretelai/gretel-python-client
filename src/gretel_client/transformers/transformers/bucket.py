from dataclasses import dataclass
from numbers import Number
from typing import Union, List

from gretel_client.transformers.base import TransformerConfig, Transformer


@dataclass(frozen=True)
class BucketCreationParams:
    """
    Bucket creation parameter container. Stores minimum-, maximum-value of range to cover and width for each bucket.
    Used to automatically create a list of buckets covering the range specified.

    Args:
        min: float or int specifying bottom of range to cover
        max: float or int specifying top of range to cover
        width: float or int specifying the width for each bucket.
    """

    min: Number = None
    max: Number = None
    width: Number = None


@dataclass(frozen=True)
class Bucket:
    """
    Bucket container. Stores minimum value, maximum value and label for the bucket.

    Args:
        min: float, int or string specifying the bottom value for this bucket
        max: float, int or string specifying the top value for this bucket
        label: float, int or string specifying the replacement value or name for this bucket
    """

    min: Union[Number, str] = None
    max: Union[Number, str] = None
    label: Union[Number, str] = None


@dataclass(frozen=True)
class BucketConfig(TransformerConfig):
    """
    Sort numeric data into buckets.  Each bucket has a numeric or string label.

    Args:
        buckets: ``Bucket`` objectof three floats or ints to specify minimum, maximum and bucket width.
            List of int or float to explicitly specify bucket boundaries.  Bucket boundaries are left inclusive.
        lower_outlier_label: Float, int or string.  This label will be applied to values greater or equal to the
            maximum bucketed value.  If None, use the first bucket label.
        upper_outlier_label: Float, int or string.  This label will be applied to values less than the
            minimum bucketed value.  If None, use the last bucket label.
    """

    buckets: List[Bucket] = None
    lower_outlier_label: Union[Number, str] = None
    upper_outlier_label: Union[Number, str] = None


def bucket_creation_params_to_list(
    bucket_creation_params: BucketCreationParams = None,
    labels: List[Union[Number, str]] = None,
    label_method: str = None,
) -> List[Bucket]:
    """
    Helper function.  Use a ``BucketCreationParams`` instance to create a list of ``Bucket`` objects used by
    ``BucketConfig``. Use it to create a concise list of buckets covering a range of integers or floats.

    Args:
        bucket_creation_params: ``BucketCreationParams`` object to specify minimum, maximum and bucket width.
        labels: (Optional) List of labels, must match length of resulting bucket list. If missing, labels
            will be automatically created.
        label_method: (Optional) if labels is None, one of 'min', 'max' or 'avg' can be specified, so that each
            bucket uses either the left or right endpoint or the average of the two as the bucket label. Default: "min"
    
    Returns:
        Explicit list of ``Bucket`` instances.
    """
    if bucket_creation_params is None:
        raise ValueError(
            "bucket_creation_params must be a BucketCreationParams(min, max, width) object!"
        )
    if labels is None:
        labels = get_bucket_labels_from_creation_params(
            bucket_creation_params, label_method
        )
    b = []
    current_min = bucket_creation_params.min
    idx = 0
    while current_min < bucket_creation_params.max:
        current_max = (
            current_min + bucket_creation_params.width
            if current_min + bucket_creation_params.width < bucket_creation_params.max
            else bucket_creation_params.max
        )
        b.append(Bucket(current_min, current_max, None if not labels else labels[idx]))
        current_min += bucket_creation_params.width
        idx += 1
    return b


def get_bucket_labels_from_creation_params(
    bucket_creation_params: BucketCreationParams = None, label_method: str = None,
) -> List[Number]:
    """
    Helper function.  Use a ``BucketCreationParams`` container to create a list of labels.  The labels can be the
    minimum, average or maximum value for each bucket.

    Args:
        bucket_creation_params: ``BucketCreationParams`` object to specify minimum, maximum and bucket width.
        label_method: One of 'min', 'max' or 'avg'.  For each bucket, use either the left or right endpoint or the
            average of the two as the bucket label. Default: "min"

    Returns:
        List of numeric bucket labels.
    """
    if bucket_creation_params is None:
        raise ValueError(
            "bucket_creation_params must be a ``BucketCreationParams`` (min, max, width) object"
        )
    # Use minimum by default
    label_method = label_method or "min"
    if label_method not in ["min", "max", "avg"]:
        raise ValueError(
            f"label_method must be one of 'min', 'max', 'avg': {label_method}"
        )
    bucket_labels = []
    current_min = bucket_creation_params.min
    while current_min < bucket_creation_params.max:
        if label_method == "min":
            bucket_labels.append(current_min)
        elif label_method == "max":
            bucket_labels.append(
                min(
                    current_min + bucket_creation_params.width,
                    bucket_creation_params.max,
                )
            )
        elif label_method == "avg":
            bucket_labels.append(current_min + (bucket_creation_params.width / 2))
        current_min += bucket_creation_params.width
    return bucket_labels


class BucketTransformer(Transformer):
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
            self.buckets[0].label
            if config.lower_outlier_label is None
            else config.lower_outlier_label
        )
        self.upper_outlier_label = (
            self.buckets[-1].label
            if config.upper_outlier_label is None
            else config.upper_outlier_label
        )

    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        try:
            return self._mutate(value)
        except (TypeError, ValueError):
            return value

    def _mutate(self, value: Union[Number, str]) -> Union[Number, str]:
        if value < self.buckets[0].min:
            return self.lower_outlier_label
        if value >= self.buckets[-1].max:
            return self.upper_outlier_label
        for idx in range(len(self.buckets)):
            if self.buckets[idx].min <= value < self.buckets[idx].max:
                return self.buckets[idx].label
