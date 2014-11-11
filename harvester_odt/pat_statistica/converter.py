# -*- coding: utf-8 -*-

import itertools
import logging

import eventlite

from harvester.utils import ProgressReport
from harvester.ext.converter.base import ConverterPluginBase
from .conv_statistica import dataset_statistica_to_ckan
from .conv_statistica_subpro import dataset_statistica_subpro_to_ckan
from .constants import ORGANIZATIONS, CATEGORIES


logger = logging.getLogger(__name__)


class StatisticaToCkan(ConverterPluginBase):
    logger = logging.getLogger(__name__)

    def convert(self, storage_in, storage_out):
        return convert_statistica_to_ckan(storage_in, storage_out)


class StatisticaSubProToCkan(ConverterPluginBase):
    logger = logging.getLogger(__name__)

    def convert(self, storage_in, storage_out):
        self.logger.debug('Converting datasets')
        for dataset_id in storage_in.documents['dataset']:
            dataset = storage_in.documents['dataset'][dataset_id]
            clean_dataset = dataset_statistica_subpro_to_ckan(dataset)
            _dsid = clean_dataset['id']
            storage_out.documents['dataset'][_dsid] = clean_dataset

        self.logger.debug('Importing groups')
        for group in CATEGORIES.itervalues():
            storage_out.documents['group'][group['name']] = group

        self.logger.debug('Importing organizations')
        for org in ORGANIZATIONS.itervalues():
            storage_out.documents['organization'][org['name']] = org


def convert_statistica_to_ckan(storage_in, storage_out):
    logger.debug('Converting data pat_statistica -> ckan')
    logger.debug('Input storage: {0}'.format(storage_in.url))
    logger.debug('Output storage: {0}'.format(storage_out.url))

    # Calculate the "total" progress
    progress_total = len(storage_in.documents['dataset'])
    progress_total += len(CATEGORIES)
    progress_total += len(ORGANIZATIONS)

    progress = itertools.count(1)
    eventlite.emit(ProgressReport(0, progress_total))

    for dataset_id, dataset in storage_in.documents['dataset'].iteritems():
        logger.debug('Importing dataset {0}'.format(dataset_id))
        clean_dataset = dataset_statistica_to_ckan(dataset)
        _dsid = clean_dataset['id']
        storage_out.documents['dataset'][_dsid] = clean_dataset
        eventlite.emit(ProgressReport(progress.next(), progress_total))

    logger.debug('Importing groups')
    for group in CATEGORIES.itervalues():
        storage_out.documents['group'][group['name']] = group
        eventlite.emit(ProgressReport(progress.next(), progress_total))

    logger.debug('Importing organizations')
    for org in ORGANIZATIONS.itervalues():
        storage_out.documents['organization'][org['name']] = org
        eventlite.emit(ProgressReport(progress.next(), progress_total))


def convert_statistica_subpro_to_ckan(storage_in, storage_out):
    logger.debug('Converting data pat_statistica_subpro -> ckan')
    logger.debug('Input storage: {0}'.format(storage_in.url))
    logger.debug('Output storage: {0}'.format(storage_out.url))

    # Calculate the "total" progress
    progress_total = len(storage_in.documents['dataset'])
    progress_total += len(CATEGORIES)
    progress_total += len(ORGANIZATIONS)

    progress = itertools.count(1)
    eventlite.emit(ProgressReport(0, progress_total))

    logger.debug('Converting datasets')
    for dataset_id, dataset in storage_in.documents['dataset'].iteritems():
        logger.debug('Importing dataset {0}'.format(dataset_id))
        clean_dataset = dataset_statistica_subpro_to_ckan(dataset)
        _dsid = clean_dataset['id']
        storage_out.documents['dataset'][_dsid] = clean_dataset
        eventlite.emit(ProgressReport(progress.next(), progress_total))

    logger.debug('Importing groups')
    for group in CATEGORIES.itervalues():
        storage_out.documents['group'][group['name']] = group
        eventlite.emit(ProgressReport(progress.next(), progress_total))

    logger.debug('Importing organizations')
    for org in ORGANIZATIONS.itervalues():
        storage_out.documents['organization'][org['name']] = org
        eventlite.emit(ProgressReport(progress.next(), progress_total))
