import logging

from harvester.ext.converters.base import ConverterPluginBase

from .client import (dataset_statistica_to_ckan,
                     dataset_statistica_subpro_to_ckan,
                     ORGANIZATIONS, CATEGORIES)


class StatisticaToCkan(ConverterPluginBase):
    logger = logging.getLogger(__name__)

    def convert(self, storage_in, storage_out):
        self.logger.debug('Converting datasets')
        for dataset_id in storage_in.list_objects('dataset'):
            dataset = storage_in.get_object('dataset', dataset_id)
            clean_dataset = dataset_statistica_to_ckan(dataset)
            storage_out.set_object(
                'dataset', clean_dataset['id'], clean_dataset)

        self.logger.debug('Importing groups')
        for group in CATEGORIES.itervalues():
            storage_out.set_object('group', group['name'], group)

        self.logger.debug('Importing organizations')
        for org in ORGANIZATIONS.itervalues():
            storage_out.set_object('organization', org['name'], org)


class StatisticaSubProToCkan(ConverterPluginBase):
    logger = logging.getLogger(__name__)

    def convert(self, storage_in, storage_out):
        self.logger.debug('Converting datasets')
        for dataset_id in storage_in.list_objects('dataset'):
            dataset = storage_in.get_object('dataset', dataset_id)
            clean_dataset = dataset_statistica_subpro_to_ckan(dataset)
            storage_out.set_object(
                'dataset', clean_dataset['id'], clean_dataset)

        self.logger.debug('Importing groups')
        for group in CATEGORIES.itervalues():
            storage_out.set_object('group', group['name'], group)

        self.logger.debug('Importing organizations')
        for org in ORGANIZATIONS.itervalues():
            storage_out.set_object('organization', org['name'], org)
