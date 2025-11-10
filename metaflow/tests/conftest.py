import pytest


@pytest.fixture
def sample_record():
    return {'id': 1, 'name': 'alice', 'active': True}
