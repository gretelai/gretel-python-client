from unittest.mock import MagicMock, patch

import pytest

from gretel_client.files.interface import File, FileClient


@pytest.mark.parametrize("which", ["post", "get"])
def test_file_post_and_get(which):
    with patch(f"gretel_client.files.interface.requests.{which}") as mock:
        mock.return_value = MagicMock(
            json=lambda: {
                "file_id": "f_1",
                "object": "file",
                "bytes": 100,
                "created_at": 123456,
                "filename": "test.txt",
                "purpose": "test",
            }
        )

        file = MagicMock(name="test.txt")
        file.read.return_value = b"test data"

        client = FileClient()

        if which == "post":
            uploaded_file = client.upload(file, "test")
        elif which == "get":
            uploaded_file = client.get("f_1")
        else:
            raise ValueError("Invalid test case")

        assert isinstance(uploaded_file, File)
        assert uploaded_file.id == "f_1"
        assert uploaded_file.bytes == 100
        assert uploaded_file.filename == "test.txt"
        assert uploaded_file.purpose == "test"


def test_file_not_exists():
    with pytest.raises(FileNotFoundError) as ex:
        client = FileClient()
        client.upload("non_existent_file.txt", "test")

    assert "Could not find file at path" in str(ex)
