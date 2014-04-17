import logging

from harvester.ext.crawlers.base import HarvesterPluginBase

from .client import (StatisticaClient, StatisticaSubproClient)


class Statistica(HarvesterPluginBase):
    """
    Crawler for statweb.provincia.tn.it (indicatori strutturali).

    Configuration options:

    - ``bruteforce_find`` (bool, default=False):
      if set to True, will use brute force to
      find datasets, instead of just listing them.
    """

    logger = logging.getLogger(__name__)

    def fetch_data(self, storage):
        self.logger.info("Fetching data from statistica")

        client = StatisticaClient()  # todo: consider the base URL?

        if self.conf.get('bruteforce_find', False):
            datasets = client.force_iter_datasets()
        else:
            datasets = client.iter_datasets()

        for dataset in datasets:
            self.logger.info('Got dataset: {0}'.format(dataset['id']))
            storage.set_object('dataset', dataset['id'], dataset)


class StatisticaSubPro(HarvesterPluginBase):
    """
    Crawler for statweb.provincia.tn.it (indicatori strutturali
    sub-provinciali).

    Configuration options:

    - ``bruteforce_find`` (bool, default=False):
      if set to True, will use brute force to
      find datasets, instead of just listing them.
    """

    logger = logging.getLogger(__name__)

    def fetch_data(self, storage):
        self.logger.info("Fetching data from statistica-subpro")

        client = StatisticaSubproClient()

        if self.conf.get('bruteforce_find', False):
            datasets = client.force_iter_datasets()
        else:
            datasets = client.iter_datasets()

        for dataset in datasets:
            self.logger.info('Dataset: {0}'.format(dataset['id']))
            storage.set_object('dataset', dataset['id'], dataset)
