import os

import pytest


@pytest.fixture(params=['sqlite', 'memory', 'jsondir', 'mongodb'])
def storage(request, tmpdir):
    if request.param == 'sqlite':
        from harvester.ext.storage.sqlite import SQLiteStorage
        myfilename = str(tmpdir.join('example.sqlite'))
        return SQLiteStorage('file://' + myfilename)

    elif request.param == 'memory':
        from harvester.ext.storage.memory import MemoryStorage
        return MemoryStorage()

    elif request.param == 'jsondir':
        # from harvester.ext.storage.jsondir import JsonDirStorage
        pytest.skip('Not implemented yet')
        pass

    elif request.param == 'mongodb':
        from harvester.ext.storage.mongodb import MongodbStorage

        # We need a configured MONGO_URL in order to run mongodb-based tests
        # Also, we want to make sure the storage is empty before running
        # tests, so we call ``flush_storage()``
        if 'MONGO_URL' in os.environ:
            mongo_url = os.environ['MONGO_URL']
            st = MongodbStorage(mongo_url)
            st.flush_storage()
            return st

        else:
            pytest.skip('MONGO_URL not configured')

    raise AssertionError('Invalid parameter')
