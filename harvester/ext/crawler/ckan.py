import logging

from harvester.ext.crawler.base import CrawlerPluginBase

from ckan_api_client.high_level import CkanHighlevelClient


class CkanCrawler(CrawlerPluginBase):
    logger = logging.getLogger(__name__)

    def fetch_data(self, storage):
        self.logger.info("Fetching data from Ckan at {0}".format(self.url))
        client = CkanHighlevelClient(
            self.url, api_key=self.conf.get('api_key'))

        for dataset in client.iter_datasets():
            self.logger.info("Dataset: {0}".format(dataset.id))
            storage.set_object('dataset', dataset.id, dataset.serialize())

        for group in client.iter_groups():
            self.logger.info("Group: {0}".format(group.name))
            storage.set_object('group', group.name, group.serialize())

        for organization in client.iter_organizations():
            self.logger.info("Organization: {0}".format(organization.name))
            storage.set_object('organization', organization.name,
                               organization.serialize())
