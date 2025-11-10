from metaflow.core.metadata_extractor import extract_metadata


def test_extract_metadata_basic():
    sample = {'id': 1, 'name': 'alice', 'active': True}
    m = extract_metadata(sample)
    assert m['id'] == 'int'
    assert m['name'] == 'str'
    assert m['active'] == 'bool'
