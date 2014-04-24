"""
Tests for the SQLite storage
"""

import pytest

from harvester.ext.storage.base import NotFound


def test_storages(storage):
    assert list(storage.documents) == []
    assert list(storage.documents['invalid_type']) == []

    with pytest.raises(NotFound):
        storage.documents['footype']['fooid']

    # Create object
    # -------------

    storage.documents['dataset']['1'] = {'title': 'Example dataset'}
    assert list(storage.documents) == ['dataset']
    assert list(storage.documents['dataset']) == ['1']
    assert storage.documents['dataset']['1'] == {'title': 'Example dataset'}

    # Update object
    # -------------

    storage.documents['dataset']['1'] = {'title': 'NEW TITLE'}
    assert list(storage.documents) == ['dataset']
    assert list(storage.documents['dataset']) == ['1']
    assert storage.documents['dataset']['1'] == {'title': 'NEW TITLE'}

    # Delete object
    # -------------

    del storage.documents['dataset']['1']
    assert list(storage.documents) == ['dataset']
    assert list(storage.documents['dataset']) == []

    with pytest.raises(NotFound):
        storage.documents['dataset']['1']
