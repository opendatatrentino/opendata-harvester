# -*- coding: utf-8 -*-

import logging

from harvester.ext.crawler.base import CrawlerPluginBase

from .client import (StatisticaClient, StatisticaSubproClient)


class Statistica(CrawlerPluginBase):
    """
    Crawler for statweb.provincia.tn.it (indicatori strutturali).

    Configuration options:

    - ``bruteforce_find`` (bool, default=False):
      if set to True, will use brute force to
      find datasets, instead of just listing them.
    """

    logger = logging.getLogger(__name__)
    options = [
        ('bruteforce_find', 'bool', False,
         'If set to True, will use brute force to find datasets, '
         'instead of just listing them.'),
    ]

    def fetch_data(self, storage):
        self.logger.info("Fetching data from statistica")

        client = StatisticaClient()  # todo: consider the base URL?

        if self.conf.get('bruteforce_find', False):
            datasets = client.force_iter_datasets()
        else:
            datasets = client.iter_datasets()

        for dataset in datasets:
            self.logger.info('Got dataset: {0}'.format(dataset['id']))
            storage.documents['dataset'][dataset['id']] = dataset

            # todo: we could download resources as blobs too..


class StatisticaSubPro(CrawlerPluginBase):
    """
    Crawler for statweb.provincia.tn.it (indicatori strutturali
    sub-provinciali).

    Configuration options:

    - ``bruteforce_find`` (bool, default=False):
      if set to True, will use brute force to
      find datasets, instead of just listing them.
    """

    logger = logging.getLogger(__name__)
    options = [
        ('bruteforce_find', 'bool', False,
         'If set to True, will use brute force to find datasets, '
         'instead of just listing them.'),
    ]

    def fetch_data(self, storage):
        self.logger.info("Fetching data from statistica-subpro")

        client = StatisticaSubproClient()

        if self.conf.get('bruteforce_find', False):
            datasets = client.force_iter_datasets()
        else:
            datasets = client.iter_datasets()

        for dataset in datasets:
            self.logger.info('Dataset: {0}'.format(dataset['id']))
            storage.documents['dataset'][dataset['id']] = dataset

            # todo: we could download resources as blobs too..
