import logging

from ckan_api_client.syncing import SynchronizationClient

from harvester.ext.importers.base import ImporterPluginBase


class CkanImporter(ImporterPluginBase):
    logger = logging.getLogger(__name__)

    def sync_data(self, storage):
        self.logger.info("Starting data synchronization to ckan")

        api_key = self.conf['api_key']
        source_name = self.conf['source_name']
        client = SynchronizationClient(self.url, api_key)

        # todo: load all data from the storage, or make it accessible as a dict
        # todo: we need to preserve names on update

        data = {'dataset': {}, 'group': {}, 'organization': {}}
        client.sync(source_name, data)
