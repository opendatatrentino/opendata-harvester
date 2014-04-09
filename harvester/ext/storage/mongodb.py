import urlparse

import pymongo

from .base import BaseStorage


class MongodbStorage(BaseStorage):
    def __init__(self, url):
        """
        MongoDB storage takes a URL like:

        mongodb://host:port/database.name/collection.name
        """
        parsed_url = urlparse.urlparse(url)
        parsed_path = parsed_url.path.strip('/').split('/')

        if len(parsed_path) < 1:
            raise ValueError("Invalid MongoDB url (missing database name)")

        self._mongo_url = parsed_url._replace(path='').geturl()
        self._mongo_db_name = parsed_path[0]

        if len(parsed_path) >= 2:
            self._mongo_collection_prefix = parsed_path[1]
        else:
            self._mongo_collection_prefix = None

    @property
    def _connection(self):
        if getattr(self, '_cached_connection', None) is None:
            self._cached_connection = pymongo.MongoClient(self._mongo_url)
        return self._cached_connection

    @property
    def _database(self):
        return self._connection[self._mongo_db_name]

    @property
    def _collection(self):
        if self._mongo_collection_prefix is not None:
            return self._database[self._mongo_collection_prefix]
        return self._database

    def _list_collections(self, strip_prefix=True):
        """
        List all collections with name starting with the
        configured prefix.
        """

        if self._mongo_collection_prefix:
            prefix = self._mongo_collection_prefix + '.'
        else:
            prefix = None

        for name in self._database.collection_names():
            if name.startswith('system.'):
                # Ignore system collections
                continue

            if prefix is None:
                yield name

            else:
                if name.startswith(prefix):
                    if strip_prefix:
                        yield name[len(prefix):]
                    else:
                        yield name

    def _flush_db(self):
        if self._mongo_collection_prefix is None:
            self._connection.drop_database(self._mongo_db_name)
        else:
            # Just drop tables matching prefix
            for name in self._list_collections(strip_prefix=False):
                self._database.drop_collection(name)

    def list_object_types(self):
        return list(self._list_collections())

    def list_objects(self, obj_type):
        if obj_type not in self.list_object_types():
            raise ValueError('Missing object')
        coll = self._collection[obj_type]
        return [o['_id'] for o in coll.find(fields=['_id'])]

    def get_object(self, obj_type, obj_id):
        coll = self._collection[obj_type]
        obj = coll.find_one(obj_id)
        obj.pop('_id', None)
        return obj

    def set_object(self, obj_type, obj_id, obj):
        coll = self._collection[obj_type]
        obj['_id'] = obj_id
        coll.update({'_id': obj['_id']}, obj, upsert=True)
        return self.get_object(obj_type, obj_id)

    def del_object(self, obj_type, obj_id):
        coll = self._collection[obj_type]
        coll.remove({'_id': obj_id})
