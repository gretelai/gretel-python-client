class InvalidSamplerConfigError(Exception):
    """Raised when the sampler config is invalid."""


class InvalidSampleTypeError(Exception):
    """Raised when the sample type is invalid."""


class ModelMetricMismatchError(Exception):
    """Raised when the metric is not compatible with the model."""


class SamplerCallbackError(Exception):
    """Raised when the user-provided sampler callback fails."""


class TunerTrialError(Exception):
    """Raised when a trial fails."""
