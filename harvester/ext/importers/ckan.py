import logging

from ckan_api_client.syncing import SynchronizationClient

from harvester.ext.importers.base import ImporterPluginBase


class CkanImporter(ImporterPluginBase):
    logger = logging.getLogger(__name__)

    options = [
        ('api_key', 'str', None, "Ckan API key"),
        ('source_name', 'str', None, "Name for the import source"),
        ('organization_merge_strategy', 'str', None, "One of: create, update"),
        ('group_merge_strategy', 'str', None, "One of: create, update"),
        ('dataset_preserve_names', 'bool', True,
         "if ``True`` (the default) will preserve old names of "
         "existing datasets"),
        ('dataset_preserve_organization', 'bool', True,
         "Preserve current dataset organization?"),
        ('dataset_group_merge_strategy', 'str', 'add',
         "One of: add, replace, preserve")
    ]

    def sync_data(self, storage):
        self.logger.info("Starting data synchronization to ckan")

        api_key = self.conf['api_key']
        source_name = self.conf['source_name']

        # ------------------------------------------------------------
        # Initialize the client taking some kwargs from configuration

        copy_conf = [
            'organization_merge_strategy',
            'group_merge_strategy',
            'dataset_preserve_names',
            'dataset_preserve_organization',
            'dataset_group_merge_strategy',
        ]
        kwargs = {}
        for k in copy_conf:
            kwargs[k] = self.conf[k]
        client = SynchronizationClient(self.url, api_key, **kwargs)

        # ------------------------------------------------------------
        # New-style storage is dict-like, no need to pre-query.

        data = {
            'dataset': storage.documents['dataset'],
            'group': storage.documents['group'],
            'organization': storage.documents['organization'],
        }

        client.sync(source_name, data)
