import abc

from ..base import PluginBase


class ImporterPluginBase(PluginBase):
    __metaclass__ = abc.ABCMeta

    def __init__(self, url, conf=None):
        self.url = url
        self.conf = conf or {}

    @abc.abstractmethod
    def sync_data(self, storage):
        pass
