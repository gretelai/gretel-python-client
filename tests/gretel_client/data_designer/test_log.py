import logging
import sys

from unittest.mock import MagicMock, patch

from gretel_client.data_designer.log import get_logger


def test_get_logger():
    logger_name = "test_logger"
    logger_level = logging.DEBUG

    with (
        patch("logging.getLogger") as mock_get_logger,
        patch("logging.StreamHandler") as mock_stream_handler,
        patch("logging.Formatter") as mock_formatter,
    ):

        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_handler = MagicMock()
        mock_stream_handler.return_value = mock_handler
        mock_formatter_instance = MagicMock()
        mock_formatter.return_value = mock_formatter_instance

        logger = get_logger(logger_name, level=logger_level)
        mock_get_logger.assert_called_once_with(logger_name)
        mock_stream_handler.assert_called_once_with(sys.stdout)
        mock_formatter.assert_called_once_with(
            "[%(asctime)s] [%(levelname)s] %(message)s", "%H:%M:%S"
        )
        mock_handler.setFormatter.assert_called_once_with(mock_formatter_instance)
        mock_logger.addHandler.assert_called_once_with(mock_handler)
        mock_logger.setLevel.assert_called_once_with(logger_level)
        assert logger == mock_logger
