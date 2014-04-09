import pytest

from harvester.ext.storage.sqlite import SQLiteStorage
from harvester.ext.storage.memory import MemoryStorage
from harvester.ext.storage.jsondir import JsonDirStorage
from harvester.ext.storage.mongodb import MongodbStorage


@pytest.fixture(params=['sqlite', 'memory', 'jsondir', 'mongodb'])
def storage(request, tmpdir):
    if request.param == 'sqlite':
        myfilename = str(tmpdir.join('example.sqlite'))
        return SQLiteStorage(myfilename)

    elif request.param == 'memory':
        return MemoryStorage()

    elif request.param == 'jsondir':
        pytest.skip('Not implemented yet')
        pass

    elif request.param == 'mongodb':
        pytest.skip('Not implemented yet')
        pass

    raise AssertionError('Invalid parameter')
