"""
Agent logger.

The Gretel agent uses separate logging infra from the CLI which
is optimized for service operations.
"""

import logging
import logging.config
import os
import time


class UTCFormatter(logging.Formatter):
    """
    Formatter that uses ISO 8601 timestamps, with UTC timezone.

    This matches format that AWS Lambda is using and it's also default for CloudWatch Logs agent.
    Sample log line: ``2021-03-03T02:03:45,110Z - INFO - gretel_core - TEST``

    See: https://docs.python.org/3/library/logging.html#logging.Formatter.formatTime
    """

    def __init__(self):
        super().__init__(
            # Use ISO 8601 timestamps.
            # Note: it requires separate millisecond section, since ``strftime`` format doesn't support milliseconds.
            # That %(msecs)d structure is defined in logging.Formatter.
            fmt="%(asctime)s.%(msecs)03dZ [%(process)d] - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )

        # change timezone used for timestamps to UTC
        self.converter = time.gmtime


def _get_default_logging_level():
    if os.getenv("GRETEL_DEBUG_LOGGING") == "yes":
        return logging.DEBUG

    return logging.INFO


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "iso8601_utc_timestamp_format": {
            "()": UTCFormatter,
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "iso8601_utc_timestamp_format",
            "level": _get_default_logging_level(),
        },
    },
    "root": {
        "handlers": ["console"],
        "level": _get_default_logging_level(),
    },
}


def configure_logging():
    """
    Configures ``logging`` handlers.

    Default configuration includes:

    - formatter (sample log record ``2021-03-03T02:03:45,110Z - INFO - gretel_core - TEST``)
    - handler writing to standard output
    """
    logging.config.dictConfig(LOGGING_CONFIG)
