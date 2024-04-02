try:
    from pandas import DataFrame as _DataFrameT
except ImportError:

    class _DataFrameT: ...  # noqa
