import mimetypes
import os
import tempfile

from io import BytesIO
from pathlib import Path
from typing import IO, BinaryIO, Optional, Union

import pandas as pd
import pyarrow.parquet
import pydantic
import requests
import smart_open

from gretel_client.config import (
    ClientConfig,
    get_data_plane_endpoint,
    get_session_config,
)
from gretel_client.errors import check_for_error_response


class File(pydantic.BaseModel):
    """Represents a file that has been uploaded/interacted with in the Gretel ecosystem."""

    id: str
    """The unique identifier for this file."""

    object: str
    """type which is always 'file' """

    bytes: int
    """The size of the file in bytes."""

    created_at: int
    """The timestamp when the file was created."""

    filename: str
    """The name of the file."""

    purpose: str
    """The purpose of the file. (i.e. dataset, pydantic model, etc.)"""


class FileClient:
    """
    A client for interacting with files in the Gretel ecosystem.

    Provides methods for uploading, deleting files.
    """

    def __init__(
        self, session: Optional[ClientConfig] = None, api_endpoint: Optional[str] = None
    ):
        """
        Initializes the FileClient.

        Args:
            session: The client session to use for API interactions, or ``None`` to use the
            default session.
        """
        # todo: we can replace both these with the GretelApiFactory
        self.session = session or get_session_config()
        self.api_endpoint = api_endpoint or get_data_plane_endpoint(self.session)

    def upload(
        self, file: Union[IO[bytes], BinaryIO, pd.DataFrame, Path, str], purpose: str
    ) -> File:
        """
        Uploads a file to Gretel.

        Args:
            file: The file to upload. This can be a file-like object, a pandas DataFrame, or a path to a file.
            purpose: The purpose of the file. (i.e. dataset, pydantic model, etc.)

        Returns:
           File: The uploaded File object containing metadata.
        """
        if isinstance(file, pd.DataFrame):
            return self._upload_df(file, purpose)

        if isinstance(file, (Path, str)):
            return self._upload_file_path(file, purpose)

        mime_type, _ = mimetypes.guess_type(file.name)
        if not mime_type:
            mime_type = "application/octet-stream"

        file_name = os.path.basename(file.name)

        response = requests.post(
            f"{self.api_endpoint}/v1/files",
            data={
                "purpose": purpose,
            },
            files={"file": (file_name, file, mime_type)},
            headers={"Authorization": self.session.api_key},
        )
        check_for_error_response(response)
        response_body = response.json()

        return File(
            id=response_body["file_id"],
            object="file",
            bytes=int(response_body["bytes"]),
            created_at=int(response_body["created_at"]),
            filename=response_body["filename"],
            purpose=response_body["purpose"],
        )

    def _upload_df(self, df: pd.DataFrame, purpose: str):
        with tempfile.NamedTemporaryFile(
            prefix="dataset_", suffix=".parquet"
        ) as tmp_file:
            df.to_parquet(tmp_file.name)
            tmp_file.seek(0)
            return self.upload(tmp_file, purpose)

    def _upload_file_path(self, file: Union[Path, str], purpose: str):
        with smart_open.open(file, "rb") as file_handle:
            return self.upload(file_handle, purpose)

    def get(self, file_id: str) -> File:
        """
        Get a file from Gretel.

        Args:
            file_id: The unique identifier of the file to retrieve from the File object

        Returns:
            File: The File object
        """
        response = requests.get(
            f"{self.api_endpoint}/v1/files/{file_id}",
            headers={"Authorization": self.session.api_key},
        )

        check_for_error_response(response)
        response_body = response.json()

        return File(
            id=response_body["file_id"],
            object="file",
            bytes=int(response_body["bytes"]),
            created_at=int(response_body["created_at"]),
            filename=response_body["filename"],
            purpose=response_body["purpose"],
        )

    def download_dataset(self, file_id: str) -> pd.DataFrame:
        """
        Download a dataset object into memory as a DataFrame.

        Assumes that the dataset file is stored in Parquet format.
        An exception will be raised if the downloaded octet-stream cannot be
        loaded as a parquet file.

        Args:
            file_id: The unique identifier of the file to download.

        Raises:
            ValueError: If returned octet-stream cannot be loaded as a
                parquet file.
        """
        response = requests.get(
            f"{self.api_endpoint}/v1/files/{file_id}/download",
            headers={"Authorization": self.session.api_key},
        )

        check_for_error_response(response)

        buffer = BytesIO(response.content)
        try:
            df = pyarrow.parquet.read_table(buffer).to_pandas()
        except Exception as exc:
            raise ValueError(
                "Error loading dataset file from parquet. Are you sure this was a dataset file?"
            ) from exc

        return df

    def delete(self, file_id: str) -> None:
        """
        Delete a file from Gretel.

        Args:
            file_id: The unique identifier of the file to delete from the File object
        """
        response = requests.delete(
            f"{self.api_endpoint}/v1/files/{file_id}",
            headers={"Authorization": self.session.api_key},
        )

        check_for_error_response(response)
