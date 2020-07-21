from dataclasses import dataclass
from typing import Union, List, Optional, Tuple

from gretel_client.transformers.base import TransformerConfig, Transformer


@dataclass(frozen=True)
class BucketConfig(TransformerConfig):
    buckets: Union[List[float], List[int], Tuple[Union[float, int], Union[float, int], Union[float, int]]] = None
    bucket_labels: Union[List[float], List[int], List[str]] = None
    lower_outlier_label: Union[float, int, str] = None
    upper_outlier_label: Union[float, int, str] = None

    @staticmethod
    def bucket_tuple_to_list(buckets: Tuple[Union[float, int], Union[float, int], Union[float, int]] = None):
        if buckets is None:
            raise ValueError(f"buckets must be a 3-Tuple (min, max, width)")
        b = []
        current_min = buckets[0]
        while current_min < buckets[1]:
            b.append(current_min)
            current_min += buckets[2]
        b.append(buckets[1])
        return b

    @staticmethod
    def get_bucket_labels_from_tuple(
            buckets: Tuple[Union[float, int], Union[float, int], Union[float, int]] = None,
            method: str = None) \
            -> Union[List[float], List[int]]:
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
    Bucket transformer replaces a field value with the label in the given value range.
    """
    config_class = BucketConfig

    def __init__(self, config: BucketConfig):
        super().__init__(config)
        # Expand tuple to list
        if type(config.buckets) == tuple:
            self.buckets = BucketConfig.bucket_tuple_to_list(config.buckets)
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
            # return the actual field if an error happened
            # most likely value not being a number
            return {field_name: field_value}

    def _transform_entity(self, label: str, value: Union[float, int]) -> Optional[Tuple[Optional[str], Union[float, int, str]]]:
        try:
            return None, self._mutate(value)
        except (TypeError, ValueError):
            # return the original value if an error happened
            # most likely value not being a number
            return None, value

    def _mutate(self, value: Union[float, int]) -> Union[float, int, str]:
        if value < self.buckets[0]:
            return self.lower_outlier_label
        elif value >= self.buckets[-1]:
            return self.upper_outlier_label
        for idx in range(len(self.buckets) - 1):
            if self.buckets[idx] <= value < self.buckets[idx + 1]:
                return self.bucket_labels[idx]
