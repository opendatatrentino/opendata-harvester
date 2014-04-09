"""
Tests for the SQLite storage
"""

import pytest


def test_storage(storage):
    assert storage.list_object_types() == []

    with pytest.raises(Exception):
        storage.list_objects('invalid_type')

    with pytest.raises(Exception):
        storage.get_object('invalid_type', 'invalid_id')

    # Create object
    # -------------

    storage.set_object('dataset', '1', {'title': 'Example dataset'})
    assert storage.list_object_types() == ['dataset']
    assert storage.list_objects('dataset') == ['1']
    assert storage.get_object('dataset', '1') == {'title': 'Example dataset'}

    # Update object
    # -------------

    storage.set_object('dataset', '1', {'title': 'NEW TITLE'})
    assert storage.list_object_types() == ['dataset']
    assert storage.list_objects('dataset') == ['1']
    assert storage.get_object('dataset', '1') == {'title': 'NEW TITLE'}

    # Delete object
    # -------------

    storage.del_object('dataset', '1')

    assert storage.list_object_types() == ['dataset']
    assert storage.list_objects('dataset') == []

    with pytest.raises(Exception):
        storage.get_object('dataset', '1')
