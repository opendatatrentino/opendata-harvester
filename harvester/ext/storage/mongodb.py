import urlparse

import pymongo

from .base import BaseStorage


class MongodbStorage(BaseStorage):
    def __init__(self, url):
        parsed_url = urlparse.urlparse(url)
        parsed_path = parsed_url.path.strip('/').split('/')
        if len(parsed_path) != 1:
            raise ValueError("Invalid MongoDB url (db name)")
        self._mongo_url = parsed_url._replace(path='').geturl()
        self._mongo_db_name = parsed_path[0]

    @property
    def _connection(self):
        if getattr(self, '_cached_connection', None) is None:
            self._cached_connection = pymongo.MongoClient(self._mongo_url)
        return self._cached_connection

    @property
    def _database(self):
        return self._connection[self._mongo_db_name]

    def _flush_db(self):
        self._connection.drop_database(self._mongo_db_name)

    def list_object_types(self):
        return [c for c in self._database.collection_names()
                if not c.startswith('system.')]

    def list_objects(self, obj_type):
        if obj_type not in self.list_object_types():
            raise ValueError('Missing object')
        coll = self._database[obj_type]
        return [o['_id'] for o in coll.find(fields=['_id'])]

    def get_object(self, obj_type, obj_id):
        coll = self._database[obj_type]
        obj = coll.find_one(obj_id)
        obj.pop('_id', None)
        return obj

    def set_object(self, obj_type, obj_id, obj):
        coll = self._database[obj_type]
        obj['_id'] = obj_id
        coll.update({'_id': obj['_id']}, obj, upsert=True)
        return self.get_object(obj_type, obj_id)

    def del_object(self, obj_type, obj_id):
        coll = self._database[obj_type]
        coll.remove({'_id': obj_id})
