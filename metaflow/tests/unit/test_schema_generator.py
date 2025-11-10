from metaflow.core.schema_generator import generate_schema


def test_generate_schema_basic():
    meta = {'id': 'int', 'value': 'float', 'x': 'str'}
    s = generate_schema(meta)
    assert s['id'] == 'INTEGER'
    assert s['value'] == 'FLOAT'
    assert s['x'] == 'STRING'
