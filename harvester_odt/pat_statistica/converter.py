from harvester.ext.converter.base import ConverterPluginBase

from .client import (dataset_statistica_to_ckan,
                     dataset_statistica_subpro_to_ckan,
                     ORGANIZATIONS, CATEGORIES)


class StatisticaToCkan(ConverterPluginBase):
    def convert(self, storage_in, storage_out):
        for dataset_id in storage_in.list_objects('dataset'):
            dataset = storage_in.get_object('dataset', dataset_id)
            clean_dataset = dataset_statistica_to_ckan(dataset)
            storage_out.set_object(
                'dataset', clean_dataset['id'], clean_dataset)

        for group in CATEGORIES.itervalues():
            storage_out.set('group', group['name'], group)

        for org in ORGANIZATIONS.itervalues():
            storage_out.set('organization', org['name'], org)


class StatisticaSubProToCkan(ConverterPluginBase):
    def convert(self, storage_in, storage_out):
        for dataset_id in storage_in.list_objects('dataset'):
            dataset = storage_in.get_object('dataset', dataset_id)
            clean_dataset = dataset_statistica_subpro_to_ckan(dataset)
            storage_out.set_object(
                'dataset', clean_dataset['id'], clean_dataset)

        for group in CATEGORIES.itervalues():
            storage_out.set('group', group['name'], group)

        for org in ORGANIZATIONS.itervalues():
            storage_out.set('organization', org['name'], org)
