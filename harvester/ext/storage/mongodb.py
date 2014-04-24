import urlparse

from pymongo import MongoClient
from gridfs import GridFS

from .base import (BaseStorage, BaseDocumentBucket, BaseBlobBucket,
                   BaseKeyvalBucket, NotFound)


class MongodbStorage(BaseStorage):
    def __init__(self, *a, **kw):
        """
        MongoDB storage takes a URL like:

        mongodb://host:port/database.name/collection.name
        """

        super(MongodbStorage, self).__init__(*a, **kw)

        # Now, extract information from the connection URL
        parsed_url = urlparse.urlparse(self.url)
        parsed_path = parsed_url.path.strip('/').split('/')

        if len(parsed_path) < 1:
            raise ValueError("Invalid MongoDB url (missing database name)")

        # Store a URL suitable for passing to MongoClient
        # i.e. remove the path
        self._mongo_url = parsed_url._replace(path='').geturl()

        # Store separately databse name and collection prefix
        self._mongo_db_name = parsed_path[0]
        self._mongo_prefix = parsed_path[1] if len(parsed_path) >= 2 else None

    @property
    def _connection(self):
        if getattr(self, '_cached_connection', None) is None:
            self._cached_connection = MongoClient(self._mongo_url)
        return self._cached_connection

    @property
    def _database(self):
        return self._connection[self._mongo_db_name]

    def _get_collection(self, name):
        """
        Return collection object, by name.
        Prefix will be prepended automatically.
        """
        name = self._get_collection_name(name)
        return self._database[name]

    def _get_collection_name(self, name):
        """
        Return a collection name, with prepended prefix.
        """
        if not isinstance(name, (list, tuple)):
            name = (name,)
        name = list(name)
        name.insert(0, self._mongo_prefix)
        return '.'.join(filter(None, name))

    def _join_prefix(self, *parts):
        _prefix = []
        for part in parts:
            if not part:
                continue
            if isinstance(part, (list, tuple)):
                _prefix.extend(part)
            else:
                _prefix.append(part)
        return _prefix

    def _list_sub_collections(self, prefix=None, strip=True):
        """
        List all the collections having the selected prefix.

        :param prefix: string or list/tuple containing prefix parts.
        :param strip: if True (default), strip prefix from names
        """

        prefix = self._join_prefix(self._mongo_prefix, prefix)
        prefix = '.'.join(prefix)
        if prefix:
            prefix += '.'

        striplen = len(prefix) if strip else 0

        for name in self._database.collection_names():
            if name.startswith('system.'):
                continue  # Ignore system collections

            if (not prefix) or name.startswith(prefix):
                yield name[striplen:]

    def _list_our_collections(self):
        """List full names of our collections"""
        return self._list_sub_collections(strip=False)

    def _list_buckets(self, bucket_name):
        return self._list_sub_collections(bucket_name)

    def flush_storage(self):
        # If no prefix was configured, we can just drop the whole
        # database. Otherwise, we need to drop our collections one-by-one.
        if self._mongo_prefix is None:
            self._connection.drop_database(self._mongo_db_name)
        else:
            for name in self._list_our_collections():
                self._database.drop_collection(name)


class BaseMongoBucket(object):
    bucket_name = None  # to be overwritten by subclasses

    @classmethod
    def list_buckets(cls, storage):
        return storage._list_buckets(cls.bucket_name)

    def _get_collection(self):
        return self.storage._get_collection([self.bucket_name, self.name])

    def __iter__(self):
        coll = self._get_collection()
        for obj in coll.find(fields=['_id']):
            yield obj['_id']

    def __len__(self):
        coll = self._get_collection()
        return coll.count()

    def __getitem__(self, name):
        coll = self._get_collection()
        obj = coll.find_one(name)
        if obj is None:
            raise NotFound('Object not found')
        obj.pop('_id', None)
        return self._deserialize(obj)

    def __setitem__(self, name, value):
        coll = self._get_collection()
        value = self._serialize(value)
        value['_id'] = name
        coll.update({'_id': value['_id']}, value, upsert=True)

    def __delitem__(self, name):
        coll = self._get_collection()
        coll.remove({'_id': name})

    def _serialize(self, obj):
        return obj

    def _deserialize(self, obj):
        return obj


class MongoDocumentBucket(BaseMongoBucket, BaseDocumentBucket):
    bucket_name = 'document'


class MongoBlobBucket(BaseMongoBucket, BaseBlobBucket):
    """MongoDB "blob" bucket uses GridFS to store binary data"""

    bucket_name = 'blob'

    def _get_gridfs(self):
        coll_name = self.storage._get_collection_name(
            [self.bucket_name, self.name])
        return GridFS(self.storage._database, collection=coll_name)

    @classmethod
    def list_buckets(cls, storage):
        # We want to list only first-level collection names.
        # Eg, we will get a list like:
        # ['foo.chunks', 'foo.files', 'bar.chunks', 'bar.files']
        # but we want: ['foo', 'bar']
        buckets = storage._list_buckets(cls.bucket_name)
        for x in set(b.split('.')[0] for b in buckets):
            yield x

    def __iter__(self):
        grid = self._get_gridfs()
        for g in grid.find():
            yield g._id

    def __len__(self):
        grid = self._get_gridfs()
        return len(grid.find())  # todo: improve this!

    def __getitem__(self, name):
        grid = self._get_gridfs()
        return grid.get(name).read()

    def __setitem__(self, name, value):
        grid = self._get_gridfs()
        grid.delete(name)
        grid.put(value, _id=name)

    def __delitem__(self, name):
        grid = self._get_gridfs()
        grid.delete(name)


class MongoKeyvalBucket(BaseMongoBucket, BaseKeyvalBucket):
    """
    MongoDB key/val bucket is similar to document bucket,
    but we need to store value inside a key of the document..
    """

    bucket_name = 'keyval'

    def _serialize(self, obj):
        return {'value': obj}

    def _deserialize(self, obj):
        return obj['value']


MongodbStorage.document_bucket_class = MongoDocumentBucket
MongodbStorage.blob_bucket_class = MongoBlobBucket
MongodbStorage.keyval_bucket_class = MongoKeyvalBucket
