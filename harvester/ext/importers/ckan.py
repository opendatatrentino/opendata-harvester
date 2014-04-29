import logging

from ckan_api_client.syncing import SynchronizationClient

from harvester.ext.importers.base import ImporterPluginBase


class CkanImporter(ImporterPluginBase):
    logger = logging.getLogger(__name__)

    options = [
        ('api_key', 'str', None),
        ('source_name', 'str', None),
    ]

    def sync_data(self, storage):
        self.logger.info("Starting data synchronization to ckan")

        api_key = self.conf['api_key']
        source_name = self.conf['source_name']
        client = SynchronizationClient(self.url, api_key)

        # todo: load all data from the storage, or make it accessible as a dict
        # todo: we need to preserve names on update
        # todo: improve the sync client to make it more extensible

        data = {'dataset': {}, 'group': {}, 'organization': {}}

        for obj_id in storage.documents['dataset']:
            obj = storage.documents['dataset'][obj_id]
            data['dataset'][obj_id] = obj

        for obj_id in storage.documents['group']:
            obj = storage.documents['group'][obj_id]
            data['group'][obj_id] = obj

        for obj_id in storage.documents['organization']:
            obj = storage.documents['organization'][obj_id]
            data['organization'][obj_id] = obj

        client.sync(source_name, data)
