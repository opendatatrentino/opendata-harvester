"""
Harvester director.

Service to keep track / schedule jobs easily.
"""

from flask import current_app
from harvester.ext.storage.mongodb import MongodbStorage


class HarvesterDirector(object):
    """
    Configuration is read from the Flask application.
    """

    def __init__(self, conf=None):
        self._conf = conf

    @property
    def conf(self):
        if self._conf is None:
            try:
                self._conf = current_app.config
            except RuntimeError:
                from harvester.director.web import app
                with app.app_context():
                    self._conf = app.config
        return self._conf

    def get_storage(self, coll_name):
        """
        Get an instance of MongodbStorage, based on the configured
        database plus the ``coll_name`` collection prefix.
        """
        storage_conf = dict(self.conf['HARVESTER_MONGODB'])  # make a copy
        db_url = storage_conf.pop('host')
        db_name = storage_conf.pop('name')
        storage = MongodbStorage(
            '/'.join((db_url, db_name, coll_name)), storage_conf)
        return storage
