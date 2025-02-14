import json
from unittest.mock import MagicMock, patch

import pytest

from DeduplicateDatasets.config import BUCKET_NAME
from DeduplicateDatasets.get_duplicate_datasets import list_obj_keys


@pytest.fixture
def mock_s3_client():
    """
    Mocks the S3 client using pre-saved JSON data.
    """
    # Load expected object keys from the JSON file
    with open(
        "tests/get_duplicate_datasets/data/expected_list_file_path_from_backup.json"
    ) as f:
        expected_obj_keys = json.load(f)

    # Create a mock S3 client
    mock_client = MagicMock()

    # Simulate AWS S3 list_objects_v2 response
    def mock_list_objects_v2(Bucket, Prefix, ContinuationToken=None):
        if Bucket != BUCKET_NAME or Prefix != "backup":
            return {"Contents": []}  # Return empty list for incorrect parameters

        return {
            "Contents": [{"Key": key} for key in expected_obj_keys],
            "NextContinuationToken": None,  # Simulating single-page response
        }

    mock_client.list_objects_v2.side_effect = mock_list_objects_v2
    return mock_client


@patch("DeduplicateDatasets.get_duplicate_datasets.s3_client", new_callable=MagicMock)
def test_deduplicate_datasets(mocked_s3_client, mock_s3_client):
    """
    Tests list_obj_keys with a mocked S3 client.
    """
    mocked_s3_client.list_objects_v2.side_effect = mock_s3_client.list_objects_v2

    obj_keys = list_obj_keys("backup", mocked_s3_client, BUCKET_NAME)

    # Load expected results from the JSON file
    with open(
        "tests/get_duplicate_datasets/data/expected_list_file_path_from_backup.json"
    ) as f:
        expected_obj_keys = json.load(f)

    assert obj_keys == expected_obj_keys, "Mismatch in retrieved S3 object keys"
