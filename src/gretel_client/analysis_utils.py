from typing import Optional

from gretel_client.dataframe import _DataFrameT

try:
    import pandas as pd

    PANDAS_IS_INSTALLED = True
except ImportError:
    PANDAS_IS_INSTALLED = False

try:
    import IPython

    IPYTHON_IS_INSTALLED = True
except ImportError:
    IPYTHON_IS_INSTALLED = False


def display_dataframe_in_notebook(
    dataframe: _DataFrameT, settings: Optional[dict] = None
) -> None:
    """Display pandas DataFrame in notebook with better settings for readability.

    This function is intended to be used in a Jupyter notebook.

    Args:
        dataframe: The pandas DataFrame to display.
        settings: Optional properties to set on the DataFrame's style.
            If None, default settings with text wrapping are used.
    """
    if not PANDAS_IS_INSTALLED:
        raise ImportError("Pandas is required to display dataframes in notebooks.")
    if not IPYTHON_IS_INSTALLED:
        raise ImportError("IPython is required to display dataframes in notebooks.")
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError(
            f"Expected `dataframe` to be of type pandas.DataFrame, "
            f"you gave {type(dataframe)}"
        )
    settings = settings or {
        "text-align": "left",
        "white-space": "normal",
        "height": "auto",
    }
    IPython.display.display(dataframe.style.set_properties(**settings))
