"""In-memory database backed by dictionaries"""

from collections import defaultdict
import copy

from .base import BaseStorage


class MemoryStorage(BaseStorage):
    def __init__(self):
        self._storage = {}

    def list_object_types(self):
        return list(self._storage)

    def list_objects(self, obj_type):
        return list(self._storage[obj_type])

    def get_object(self, obj_type, obj_id):
        return self._storage[obj_type][obj_id]

    def set_object(self, obj_type, obj_id, obj):
        if obj_type not in self._storage:
            self._storage[obj_type] = {}
        self._storage[obj_type][obj_id] = copy.deepcopy(obj)

    def del_object(self, obj_type, obj_id):
        try:
            del self._storage[obj_type][obj_id]
        except KeyError:
            pass
