"""
Tests for the SQLite storage
"""

from harvester.ext.storage.sqlite import SQLiteStorage
from harvester.ext.storage.memory import MemoryStorage
from harvester.ext.storage.jsondir import JsonDirStorage
from harvester.ext.storage.mongodb import MongodbStorage

import pytest


def _test_storage(storage):
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


def test_sqlite_storage(tmpdir):
    myfilename = str(tmpdir.join('example.sqlite'))
    storage = SQLiteStorage(myfilename)
    _test_storage(storage)


def test_memory_storage():
    storage = MemoryStorage()
    _test_storage(storage)


@pytest.mark.skipif(True, reason='Not implemented yet', run=False)
def test_jsondir_storage(tmpdir):
    pass


@pytest.mark.skipif(True, reason='Not implemented yet', run=False)
def test_mongodb_storage():
    # todo: need a mongodb URL passed through environment
    # otherwise, we need to skip/xfail this test
    pass
