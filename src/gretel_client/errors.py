"""
Custom Exceptions
"""


class GretelClientError(Exception):
    pass


class GretelDependencyError(GretelClientError):
    pass
