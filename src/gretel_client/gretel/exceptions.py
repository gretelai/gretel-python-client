class GretelHighLevelInterfaceError(Exception):
    """Base exception for all high-level Gretel interface errors."""


class ModelConfigReadError(GretelHighLevelInterfaceError):
    """Raised when the base config is an invalid name or path"""


class ConfigSettingError(GretelHighLevelInterfaceError):
    """Raised when a config section or setting format is invalid."""


class GretelJobSubmissionError(GretelHighLevelInterfaceError):
    """Raised when a Gretel job submission is fails."""


class GretelJobResultsError(GretelHighLevelInterfaceError):
    """Raised when a Gretel job results cannot be fetched."""


class GretelProjectNotSetError(GretelHighLevelInterfaceError):
    """Raised when a Gretel project is not set."""


class InvalidYamlError(Exception):
    """Raised when a loaded YAML is invalid."""
