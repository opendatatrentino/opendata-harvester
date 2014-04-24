import os

import pytest


@pytest.fixture(params=['sqlite', 'memory', 'jsondir', 'mongodb'])
def storage(request, tmpdir):
    if request.param == 'sqlite':
        pytest.skip('Not implemented yet')
        from harvester.ext.storage.sqlite import SQLiteStorage
        myfilename = str(tmpdir.join('example.sqlite'))
        return SQLiteStorage(myfilename)

    elif request.param == 'memory':
        from harvester.ext.storage.memory import MemoryStorage
        return MemoryStorage()

    elif request.param == 'jsondir':
        # from harvester.ext.storage.jsondir import JsonDirStorage
        pytest.skip('Not implemented yet')
        pass

    elif request.param == 'mongodb':
        pytest.skip('Not implemented yet')
        from harvester.ext.storage.mongodb import MongodbStorage
        if 'MONGO_URL' in os.environ:
            mongo_url = os.environ['MONGO_URL']
            st = MongodbStorage(mongo_url)
            st._flush_db()
            return st

        else:
            pytest.skip('MONGO_URL not configured')

    raise AssertionError('Invalid parameter')
