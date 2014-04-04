from .base import BaseStorage


class MongodbStorage(BaseStorage):
    def list_object_types(self):
        raise NotImplementedError

    def list_objects(self, obj_type):
        raise NotImplementedError

    def get_object(self, obj_type, obj_id):
        raise NotImplementedError

    def set_object(self, obj_type, obj_id, obj):
        raise NotImplementedError

    def del_object(self, obj_type, obj_id):
        raise NotImplementedError
