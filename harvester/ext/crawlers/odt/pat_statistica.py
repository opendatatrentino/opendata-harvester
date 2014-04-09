import logging

from harvester.ext.crawlers.base import HarvesterPluginBase

from .pat_statistica_client import (StatisticaClient, StatisticaSubproClient,
                                    CATEGORIES, ORGANIZATIONS,
                                    dataset_statistica_to_ckan,
                                    dataset_statistica_subpro_to_ckan)


class Statistica(HarvesterPluginBase):
    logger = logging.getLogger(__name__)

    def fetch_data(self, storage):
        self.logger.info("Fetching data from statistica")

        client = StatisticaClient()
        for dataset in client.iter_datasets(clean=False):
            self.logger.info('Got dataset: {0}'.format(dataset['id']))
            storage.set_object('dataset', dataset['id'], dataset)

    def to_ckan(self, storage_in, storage_out):
        pass


class StatisticaSubPro(HarvesterPluginBase):
    logger = logging.getLogger(__name__)

    def fetch_data(self, storage):
        self.logger.info("Fetching data from statistica-subpro")

        client = StatisticaSubproClient()
        for dataset in client.iter_datasets(clean=False):
            self.logger.info('Dataset: {0}'.format(dataset['id']))
            storage.set_object('dataset', dataset['id'], dataset)

    def to_ckan(self, storage_in, storage_out):
        pass
