"""
Base objects for the harvester plugins.
"""

import abc


class HarvesterPluginBase(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, url, conf=None):
        self.url = url
        self.conf = conf or {}

    @abc.abstractmethod
    def fetch_data(self, collection):
        """
        Download data into a collection.

        We expect to be able to manipulate the collection as it
        were a dict of dicts::

            {
              "object-type": {
                "object-id": { ..the object.. }
              }
            }
        """
        pass

    @abc.abstractmethod
    def clean_data(self, in_collection, out_collection):
        """
        Clean raw data into another collection, ready for importing
        in ckan using the "synchronization" client.
        """
        pass
