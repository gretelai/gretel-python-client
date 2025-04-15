import json
import logging

from pathlib import Path
from typing import Callable, IO, Literal, Optional, Union

import pandas as pd

from rich.table import Table
from typing_extensions import Self

logger = logging.getLogger(__name__)

OutputDownloaderT = Callable[[Optional[Literal["json", "html"]]], IO]


class Dataset:

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    @classmethod
    def from_bytes(cls, parquet_bytes: IO) -> Self:
        return cls(pd.read_parquet(parquet_bytes))

    @classmethod
    def from_records(cls, records: list[dict]) -> Self:
        return cls(pd.DataFrame.from_records(records))

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    def download(
        self, file: Union[str, Path, IO], format: Literal["csv", "parquet"] = "parquet"
    ) -> None:
        if isinstance(file, (str, Path)):
            file = Path(file)
            file.parent.mkdir(parents=True, exist_ok=True)

        if format == "csv":
            self._df.to_csv(file, index=False)
        else:
            self._df.to_parquet(file, index=False)


class Report:

    def __init__(self, report_dict: dict, report_downloader: OutputDownloaderT):
        self._report_dict = report_dict
        self._report_downloader = report_downloader

    @classmethod
    def from_bytes(cls, report_bytes: IO, report_downloader: OutputDownloaderT) -> Self:
        byte_str = report_bytes.read()
        try:
            return cls(json.loads(byte_str), report_downloader)
        except json.JSONDecodeError as ex:
            logger.error(f"Could not deserialize report from json: {ex}")
            logger.error(f"Report contents: {byte_str}")
            raise ex

    @property
    def table(self) -> Table:
        table = Table(
            show_header=False,
            border_style="medium_purple1",
            show_lines=True,
        )

        for key, value in self.dict.items():
            table.add_row(str(key), str(value))

        return table

    @property
    def dict(self) -> dict:
        return self._report_dict

    def download(
        self, file: Union[str, Path, IO], format: Literal["json", "html"] = "html"
    ):
        if isinstance(file, IO):
            return self._report_downloader(format)

        if isinstance(file, (str, Path)):
            file_path = Path(file)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(file_path, "w") as f:
                f.write(self._report_downloader(format).read())


class PydanticModel:

    def __init__(self, model_dict: dict):
        self._model_dict = model_dict

    @classmethod
    def from_bytes(cls, report_bytes: IO) -> Self:
        return cls(json.loads(report_bytes.read()))

    @property
    def as_dict(self) -> dict:
        return self._model_dict
