from metaflow.core.metadata_extractor import extract_metadata


def test_extract_metadata_basic(sample_record):
    m = extract_metadata(sample_record)
    assert m['id'] == 'int'
    assert m['name'] == 'str'
    assert m['active'] == 'bool'
