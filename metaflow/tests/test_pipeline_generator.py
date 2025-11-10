from metaflow.core.pipeline_generator import generate_pipeline


def test_generate_pipeline_minimal():
    schema = {'id': 'INTEGER'}
    cfg = {'steps': ['read', 'write']}
    p = generate_pipeline(schema, cfg)
    assert p['schema'] == schema
    assert p['config']['steps'] == ['read', 'write']
