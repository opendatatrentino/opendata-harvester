import collections


class StorageError(Exception):
    pass


class NotFound(StorageError):
    pass


class BaseStorage(object):
    """
    Storages expose a common API to store data in
    different backends.

    A storage should be able to manage three kinds of
    objects:

    - **document**: a JSON object
    - **blob**: a large object, to be stored as-is
    - **keyval**: a key/value pair, usually for storing
      information about the storage data itself.

    The (new) API for accessing objects is:

    storage.documents <-- dict-like
    storage.blobs <-- dict-like
    storage.keyval <-- dict-like
    storage.info <-- dict-like (alias for ``.keyval['info']``)
    """

    document_bucket_class = None
    blob_bucket_class = None
    keyval_bucket_class = None

    def __init__(self, url, conf=None):
        self.url = url
        self.conf = conf or {}

    @property
    def documents(self):
        return BaseBucketManager(self, self.document_bucket_class)

    @property
    def blobs(self):
        return BaseBucketManager(self, self.blob_bucket_class)

    @property
    def keyvals(self):
        return BaseBucketManager(self, self.keyval_bucket_class)

    @property
    def info(self):
        return self.keyval['info']

    def flush_storage(self):
        raise NotImplementedError(
            "This storage does not support flushing")


class BaseBucketManager(collections.MutableMapping):
    """
    A bucket manager simulate a dictionary of buckets
    for a given storage.
    """

    def __init__(self, storage, bucket_class):
        self.storage = storage
        self.bucket_class = bucket_class

    def __getitem__(self, bucket_name):
        return self.bucket_class(self.storage, bucket_name)

    def __setitem__(self, name, value):
        raise NotImplementedError("Setting a bucket is not supported")

    def __delitem__(self, name):
        raise NotImplementedError("Deleting a bucket is not supported")

    def __iter__(self):
        return self.bucket_class.list_buckets(self.storage)

    def __len__(self):
        return len(list(self.__iter__()))


class BaseBucket(collections.MutableMapping):
    """
    Base for bucket objects, which are dict-likes mapping
    keys to objects.

    Methods left abstract / needing implementation:

    - classmethod: list_buckets(cls, storage)
    - __getitem__, __setitem__, __delitem__, __iter__, __len__
    """

    def __init__(self, storage, myname):
        self.storage = storage
        self.myname = myname

    @classmethod
    def list_buckets(cls, storage):
        """List buckets of this type in a given storage"""
        raise NotImplementedError


class BaseDocumentBucket(BaseBucket):
    pass


class BaseBlobBucket(BaseBucket):
    pass


class BaseKeyvalBucket(BaseBucket):
    pass


BaseStorage.document_bucket_class = BaseDocumentBucket
BaseStorage.blob_bucket_class = BaseBlobBucket
BaseStorage.keyval_bucket_class = BaseKeyvalBucket
