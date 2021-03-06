"""
Base objects for the harvester plugins.
"""

import abc

from ..base import PluginBase


class CrawlerPluginBase(PluginBase):
    __metaclass__ = abc.ABCMeta

    def __init__(self, url, conf=None):
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
