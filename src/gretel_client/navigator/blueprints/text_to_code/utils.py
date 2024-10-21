from typing import Optional, Union

import pandas as pd

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

console = Console()


def display_nl2code_sample(
    lang: str,
    record: Union[dict, pd.Series],
    contextual_columns: list[str],
    theme: str = "dracula",
    background_color: Optional[str] = None,
):
    if isinstance(record, (dict, pd.Series)):
        record = pd.DataFrame([record]).iloc[0]
    else:
        raise ValueError("record must be a dictionary or pandas Series")

    table = Table(title="Contextual Columns")

    for col in contextual_columns:
        table.add_column(col.replace("_", " ").capitalize())
    table.add_row(*[str(record[col]) for col in contextual_columns])

    console.print(table)

    panel = Panel(
        Text(record.text, justify="left", overflow="fold"),
        title="Text",
    )
    console.print(panel)

    panel = Panel(
        Syntax(
            record.code,
            lexer=lang.lower(),
            theme=theme,
            word_wrap=True,
            background_color=background_color,
        ),
        title="Code",
    )
    console.print(panel)
