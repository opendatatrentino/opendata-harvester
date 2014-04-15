import abc
# import collections


class StorageError(Exception):
    pass


class NotFound(StorageError):
    pass


class BaseStorage(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, url, conf=None):
        self.url = url
        self.conf = conf or {}

    @abc.abstractmethod
    def list_object_types(self):
        pass

    @abc.abstractmethod
    def list_objects(self, obj_type):
        pass

    @abc.abstractmethod
    def get_object(self, obj_type, obj_id):
        pass

    @abc.abstractmethod
    def set_object(self, obj_type, obj_id, obj):
        pass

    @abc.abstractmethod
    def del_object(self, obj_type, obj_id):
        pass

    def iter_objects(self, obj_type):
        for objid in self.list_objects(obj_type):
            yield self.get_object(obj_type, objid)

    # todo: we need to store metadata too


# We need the objects below in order to use the sync client


# class StorageDatabase(collections.MutableMapping):
#     def __init__(self, storage):
#         self.storage = storage

#     def __getitem__(self, name):
#         return StorageCollection(self, name)

#     def __setitem__(self, name, value):
#         raise NotImplementedError("Cannot set collection")

#     def __delitem__(self, name):
#         raise NotImplementedError("Cannot delete collection")

#     def __iter__(self):
#         return iter(self.storage.list_object_types())

#     def __len__(self):
#         return len(self.storage.list_object_types())


# class StorageCollection(collections.MutableMapping):
#     def __init__(self, parent, name):
#         self.parent = parent
#         self.name = name

#     @property
#     def storage(self):
#         return self.parent.storage

#     def __getitem__(self, name):
#         return self.storage.get_object(self.name, name)

#     def __setitem__(self, name, value):
#         return self.storage.set_object(self.name, name, value)

#     def __delitem__(self, name):
#         return self.storage.del_object(self.name, name)

#     def __iter__(self):
#         return iter(self.storage.list_objects(self.name))

#     def __len__(self):
#         return len(self.storage.list_objects(self.name))
