from DeduplicateDatasets.deduplicate_datasets import add_one


def test_add_one():
    assert add_one(1) == 2
