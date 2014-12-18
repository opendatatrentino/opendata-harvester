# -*- coding: utf-8 -*-

import itertools
import logging

import eventlite

from harvester.utils import report_progress
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
    return _convert_statistica_to_ckan(
        storage_in, storage_out, 'pat_statistica',
        dataset_statistica_to_ckan)


def convert_statistica_subpro_to_ckan(storage_in, storage_out):
    return _convert_statistica_to_ckan(
        storage_in, storage_out, 'pat_statistica_subpro',
        dataset_statistica_subpro_to_ckan)


def _convert_statistica_to_ckan(storage_in, storage_out,
                                source_name,
                                convert_dataset):
    logger.debug('Converting data {0} -> ckan'.format(source_name))
    logger.debug('Input storage: {0}'.format(storage_in.url))
    logger.debug('Output storage: {0}'.format(storage_out.url))

    _total = len(storage_in.documents['dataset'])
    _items = storage_in.documents['dataset'].iteritems()
    for i, (dataset_id, dataset) in enumerate(_items):
        logger.debug('Converting dataset {0}'.format(dataset_id))
        clean_dataset = convert_dataset(dataset)
        _dsid = clean_dataset['id']
        storage_out.documents['dataset'][_dsid] = clean_dataset
        report_progress(('datasets',), i + 1, _total)

    logger.debug('Converting groups')
    _total = len(CATEGORIES)
    for i, group in enumerate(CATEGORIES.itervalues()):
        storage_out.documents['group'][group['name']] = group
        report_progress(('groups',), i + 1, _total)

    logger.debug('Converting organizations')
    _total = len(ORGANIZATIONS)
    for i, org in enumerate(ORGANIZATIONS.itervalues()):
        storage_out.documents['organization'][org['name']] = org
        report_progress(('organizations',), i + 1, _total)
