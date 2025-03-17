import mimetypes
import os
import tempfile

from pathlib import Path
from typing import BinaryIO, IO, Optional, Union

import pandas as pd
import pydantic
import requests

from gretel_client.config import (
    ClientConfig,
    get_data_plane_endpoint,
    get_session_config,
)


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

    def __init__(self, session: Optional[ClientConfig] = None):
        """
        Initializes the FileClient.

        Args:
            session: The client session to use for API interactions, or ``None`` to use the
            default session.
        """
        self.session = session or get_session_config()
        self.api_endpoint = get_data_plane_endpoint(self.session)

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
        if isinstance(file, (Path, str)):
            file = Path(file)

        if not file.is_file():
            raise FileNotFoundError(
                f"Could not find file at path: {file}, please check the path and try again. If you want to upload the "
                "string contents as a file, please encode that string as bytes."
            )

        with open(file, "rb") as file_handle:
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

        response.raise_for_status()

        response_body = response.json()

        return File(
            id=response_body["file_id"],
            object="file",
            bytes=int(response_body["bytes"]),
            created_at=int(response_body["created_at"]),
            filename=response_body["filename"],
            purpose=response_body["purpose"],
        )

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

        response.raise_for_status()
