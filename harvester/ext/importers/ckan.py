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
        # todo: improve the sync client to make it more extensible

        data = {'dataset': {}, 'group': {}, 'organization': {}}

        for obj_id in storage.list_objects('dataset'):
            obj = storage.get_object('dataset', obj_id)
            data['dataset'][obj_id] = obj

        for obj_id in storage.list_objects('group'):
            obj = storage.get_object('group', obj_id)
            data['group'][obj_id] = obj

        for obj_id in storage.list_objects('organization'):
            obj = storage.get_object('organization', obj_id)
            data['organization'][obj_id] = obj

        client.sync(source_name, data)
