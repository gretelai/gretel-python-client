import logging

from rich.console import Console
from rich.logging import RichHandler
from rich.theme import Theme


def get_logger(name: str, *, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.propagate = False
    rich_handler = RichHandler(
        console=Console(theme=Theme({"logging.level.info": "green"}))
    )
    rich_handler.setFormatter(logging.Formatter("%(message)s", datefmt="[%X]"))
    logger.addHandler(rich_handler)
    logger.setLevel(logging.INFO)
    return logger
