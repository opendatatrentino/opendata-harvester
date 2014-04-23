import urlparse

import pymongo

from .base import (BaseStorage, BaseDocumentBucket, BaseBlobBucket,
                   BaseKeyvalBucket)


class MongodbStorage(BaseStorage):
    def __init__(self, url, conf=None):
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

        self.conf = conf

    @property
    def _connection(self):
        if getattr(self, '_cached_connection', None) is None:
            self._cached_connection = pymongo.MongoClient(self._mongo_url)
        return self._cached_connection

    @property
    def _database(self):
        return self._connection[self._mongo_db_name]

    def _get_collection(self, name):
        """Return a sub-collection object, by name"""
        prefix = self._mongo_collection_prefix
        root = self._database[prefix] if prefix else self._database
        return root[name]

    def _get_collection_name(self, name):
        if self._mongo_collection_prefix:
            return '.'.join((self._mongo_collection_prefix, name))
        return name

    def _list_sub_collections(self, prefix, strip_prefix=True):
        """
        List all collections with name starting with the
        configured prefix.

        :param strip_prefix:
            if set to True (the default) will strip the prefix from returned
            collection names
        """

        prefix = (prefix + '.') if prefix else None

        for name in self._database.collection_names():
            if name.startswith('system.'):
                # Ignore system collections
                continue

            if prefix is None:
                yield name

            elif name.startswith(prefix):
                if strip_prefix:
                    name = name[len(prefix):]
                yield name

    def _list_our_collections(self):
        """List full names of our collection"""

        prefix = self._mongo_collection_prefix
        prefix = (prefix + '.') if prefix else None

        for name in self._database.collection_names():
            if name.startswith('system.'):
                # Ignore system collections
                continue

            if (prefix is None) or name.startswith(prefix):
                yield name

    def _list_buckets(self, bucket_type):
        """List all the sub-collections matching a given prefix"""

        full_prefix = bucket_type + '.'
        if self._mongo_collection_prefix:
            full_prefix = '.'.join((
                self._mongo_collection_prefix, full_prefix))

        for name in self._database.collection_names():
            if name.startswith('system.'):
                # Skip system collections..
                continue

            if name.startswith(full_prefix):
                yield name[len(full_prefix):]

    # def _list_collections(self, strip_prefix=True):
    #     return self._list_sub_collections(
    #         self._mongo_collection_prefix, strip_prefix=strip_prefix)

    # def flush_storage(self):
    #     if self._mongo_collection_prefix is None:
    #         # We can directly drop database
    #         self._connection.drop_database(self._mongo_db_name)

    #     else:
    #         # Just drop tables matching prefix..
    #         for name in self._list_collections(strip_prefix=False):
    #             self._database.drop_collection(name)

    # def list_object_types(self):
    #     return list(self._list_collections())

    # def list_objects(self, obj_type):
    #     if obj_type not in self.list_object_types():
    #         raise ValueError('Missing object')
    #     coll = self._collection[obj_type]
    #     return [o['_id'] for o in coll.find(fields=['_id'])]

    # def get_object(self, obj_type, obj_id):
    #     coll = self._collection[obj_type]
    #     obj = coll.find_one(obj_id)
    #     obj.pop('_id', None)
    #     return obj

    # def set_object(self, obj_type, obj_id, obj):
    #     coll = self._collection[obj_type]
    #     obj['_id'] = obj_id
    #     coll.update({'_id': obj['_id']}, obj, upsert=True)
    #     return self.get_object(obj_type, obj_id)

    # def del_object(self, obj_type, obj_id):
    #     coll = self._collection[obj_type]
    #     coll.remove({'_id': obj_id})


class MongoDocumentBucket(BaseDocumentBucket):
    @classmethod
    def list_buckets(cls, storage):
        """
        To list buckets we want to list all the sub-collections
        matching the 'document.' prefix.
        """
        return storage._list_buckets('document')

    @property
    def _collection_name(self):
        """Get the name of the collection name for this kind of documents"""
        return self.storage._get_collection_name(
            '.'.join(('document', self.myname)))

    @property
    def _collection(self):
        return self.storage.get_dataset

    def __iter__(self):
        if self.mytype not in self.list_buckets(self.storage):
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


MongodbStorage.document_bucket_class = MongoDocumentBucket
