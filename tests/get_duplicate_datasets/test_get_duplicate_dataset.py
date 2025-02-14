import json

from DeduplicateDatasets.config import BUCKET_NAME, s3_client
from DeduplicateDatasets.get_duplicate_datasets import list_obj_keys


def test_deduplicate_datasets():
    """
    Fetches object keys from the 'backup' folder in S3 and saves them to a JSON file for testing.
    """
    obj_keys = list_obj_keys("backup", s3_client, BUCKET_NAME)
    with open(
        "tests/get_duplicate_datasets/data/expected_list_file_path_from_backup.json"
    ) as f:
        expected_obj_keys = json.load(f)
    assert obj_keys == expected_obj_keys
