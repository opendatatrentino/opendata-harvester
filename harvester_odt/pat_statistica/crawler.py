import logging

from harvester.ext.crawlers.base import HarvesterPluginBase

from .client import (StatisticaClient, StatisticaSubproClient)


class Statistica(HarvesterPluginBase):
    logger = logging.getLogger(__name__)

    def fetch_data(self, storage):
        self.logger.info("Fetching data from statistica")

        client = StatisticaClient()
        for dataset in client.iter_datasets():
            self.logger.info('Got dataset: {0}'.format(dataset['id']))
            storage.set_object('dataset', dataset['id'], dataset)


class StatisticaSubPro(HarvesterPluginBase):
    logger = logging.getLogger(__name__)

    def fetch_data(self, storage):
        self.logger.info("Fetching data from statistica-subpro")

        client = StatisticaSubproClient()
        for dataset in client.iter_datasets():
            self.logger.info('Dataset: {0}'.format(dataset['id']))
            storage.set_object('dataset', dataset['id'], dataset)
