"""In-memory database backed by dictionaries"""

import json

from .base import (BaseStorage, NotFound, BaseDocumentBucket,
                   BaseBlobBucket, BaseKeyvalBucket)


class BaseMemoryBucket(object):
    bucket_type = None  # to be overwritten in subclasses

    def __init__(self, *a, **kw):
        super(BaseMemoryBucket, self).__init__(*a, **kw)
        if getattr(self.storage, '_data', None) is None:
            self.storage._data = {}

    def _ensure_bucket_storage(self):
        if self.bucket_type not in self.storage._data:
            self.storage._data[self.bucket_type] = {}
        if self.name not in self.storage._data[self.bucket_type]:
            self.storage._data[self.bucket_type][self.name] = {}

    @classmethod
    def list_buckets(cls, storage):
        try:
            bucket_names = storage._data[cls.bucket_type].iterkeys()
        except KeyError:
            bucket_names = []

        # Explicitly return a generator!
        for name in bucket_names:
            yield name

    def __getitem__(self, name):
        try:
            raw = self.storage._data[self.bucket_type][self.name][name]
        except KeyError:
            raise NotFound("Object not found: {0!r}/{1!r}/{2!r}"
                           .format(self.bucket_type, self.name, name))
        return self._deserialize(raw)

    def __setitem__(self, name, value):
        self._ensure_bucket_storage()
        serialized = self._serialize(value)
        self.storage._data[self.bucket_type][self.name][name] = serialized

    def __delitem__(self, name):
        try:
            del self.storage._data[self.bucket_type][self.name][name]
        except KeyError:
            pass

    def __iter__(self):
        try:
            return iter(self.storage._data[self.bucket_type][self.name])
        except KeyError:
            return iter([])

    def __len__(self):
        try:
            return len(self.storage._data[self.bucket_type][self.name])
        except KeyError:
            return 0

    def _serialize(self, obj):
        # We store values as json by default, in order to make sure
        # 1. they are json-serializable objects, suitable for other
        #    storages too
        # 2. no references to mutable objects are kept around, causing
        #    misbehaviors..
        return json.dumps(obj)

    def _deserialize(self, obj):
        # By deserializing from json each time we make sure we return
        # fresh objects each time, w/o worrying about mutating objects
        return json.loads(obj)


class MemoryDocumentBucket(BaseMemoryBucket, BaseDocumentBucket):
    bucket_type = 'document'


class MemoryKeyvalBucket(BaseMemoryBucket, BaseKeyvalBucket):
    bucket_type = 'keyval'


class MemoryBlobBucket(BaseMemoryBucket, BaseBlobBucket):
    bucket_type = 'blob'

    def _serialize(self, obj):
        if not isinstance(obj, basestring):
            raise TypeError("Blob storage can only process strings")

        # We only want to store "bytes" objects, not unicode strings,
        # so we just encode them to utf-8 in case we encounter one..
        if isinstance(obj, unicode):
            obj = obj.encode('utf-8')

        return obj

    def _deserialize(self, obj):
        return obj


class MemoryStorage(BaseStorage):
    document_bucket_class = MemoryDocumentBucket
    blob_bucket_class = MemoryBlobBucket
    keyval_bucket_class = MemoryKeyvalBucket

    def __init__(self, *a, **kw):
        super(MemoryStorage, self).__init__(*a, **kw)
        self._data = {}  # Initialize storage space..

    def flush_storage(self):
        self._data = {}
