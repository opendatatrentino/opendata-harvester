# -*- coding: utf-8 -*-

import logging

import eventlite

from harvester.ext.crawler.base import CrawlerPluginBase
from harvester.utils import report_progress

from .client import (StatisticaClient, StatisticaSubproClient)


logger = logging.getLogger(__name__)


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
        return crawl_statistica(storage)


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
        return crawl_statistica_subpro(storage)


def crawl_statistica(storage):
    client = StatisticaClient()
    return _crawl_statistica(client, storage)


def crawl_statistica_subpro(storage):
    client = StatisticaSubproClient()
    return _crawl_statistica(client, storage)


def _crawl_statistica(client, storage):
    total = len(client.list_datasets())
    logger.debug('Found {0} datasets'.format(total))

    datasets = client.iter_datasets()
    for i, dataset in enumerate(datasets):
        report_progress(('datasets',), i, total,
                        'Downloading dataset: {0}'
                        .format(dataset['id']))
        logger.info('Got dataset #{0}: {1}'.format(i, dataset['id']))
        storage.documents['dataset'][dataset['id']] = dataset

    report_progress(('datasets',), total, total)

    return storage.url
