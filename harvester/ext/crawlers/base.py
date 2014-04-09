"""
Base objects for the harvester plugins.
"""

import abc


class HarvesterPluginBase(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, url, conf=None):
        # todo: configuration can be read from URL fragment
        self.url = url
        self.conf = conf or {}

    @abc.abstractmethod
    def fetch_data(self, storage):
        """
        Download data into a storage.

        We expect to be able to manipulate the storage as it
        were a dict of dicts::

            {
              "object-type": {
                "object-id": { ..the object.. }
              }
            }
        """
        pass
