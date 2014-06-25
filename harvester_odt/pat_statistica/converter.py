# -*- coding: utf-8 -*-

import logging

from harvester.ext.converter.base import ConverterPluginBase

from .conv_statistica import dataset_statistica_to_ckan
from .conv_statistica_subpro import dataset_statistica_subpro_to_ckan
from .constants import ORGANIZATIONS, CATEGORIES


class StatisticaToCkan(ConverterPluginBase):
    logger = logging.getLogger(__name__)

    def convert(self, storage_in, storage_out):
        self.logger.debug('Converting datasets')
        for dataset_id in storage_in.documents['dataset']:
            dataset = storage_in.documents['dataset'][dataset_id]
            clean_dataset = dataset_statistica_to_ckan(dataset)
            _dsid = clean_dataset['id']
            storage_out.documents['dataset'][_dsid] = clean_dataset

        self.logger.debug('Importing groups')
        for group in CATEGORIES.itervalues():
            storage_out.documents['group'][group['name']] = group

        self.logger.debug('Importing organizations')
        for org in ORGANIZATIONS.itervalues():
            storage_out.documents['organization'][org['name']] = org


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
