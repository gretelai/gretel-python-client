import logging
import sys


def get_logger(name: str, *, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.propagate = False
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger
