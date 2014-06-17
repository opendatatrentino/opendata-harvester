# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import datetime
import logging

import requests

from harvester.ext.converter.base import ConverterPluginBase
from harvester.utils import slugify

logger = logging.getLogger(__name__)

DATE_FORMAT = '%F %T'


class ComunWebToCkan(ConverterPluginBase):
    """
    Convert from comunweb opendata datasets to Ckan.

    - Each ``open_data`` object is a dataset
    - Each website has an extra dataset pointing to the API root
    - The organization to be associated with the dataset can be chosen
      via options from the command line, as it depends on the comune
      being crawled.

    """
    # todo: we need to store some stuff in the ``info`` keyvalue store
    #       in order to prevent mistakes.

    options = [
        ('org_name', 'str', None, 'Organization name'),
        ('org_title', 'str', None, 'Organization title'),
        ('org_description', 'str', None, 'Organization description'),
        ('org_image_url', 'str', None, 'Organization image url'),
    ]

    def convert(self, storage_in, storage_out):
        logger.info('Converting datasets ComunWeb -> ckan')

        # We use the open_data datasets as Ckan datasets
        objects = storage_in.documents['open_data'].iteritems()
        for dataset_id, dataset in objects:
            ckan_dataset = self._comunweb_dataset_to_ckan(dataset)
            dataset_id = str(dataset_id)  # We want string IDs
            storage_out.documents['dataset'][dataset_id] = ckan_dataset

        # We need to create the only organization we have
        logger.info('Creating organization: {0} ({1})'
                    .format(self.conf['org_name'], self.conf['org_title']))
        storage_out.documents['organization'][self.conf['org_name']] = {
            'name': self.conf['org_name'],
            'title': self.conf['org_title'],
            'description': self.conf['org_description'],
            'image_url': self.conf['org_image_url']
            or 'http://dati.trentino.it/images/logo.png',
            'type': 'organization',
            'is_organization': True,
            'state': 'active',
            'tags': [],
        }

    def _get_cached_license(self, license_url):
        cache = getattr(self, '_cached_licenses', None)

        if cache is None:
            self._cached_licenses = cache = {}

        if license_url in cache:
            return cache[license_url]

        response = requests.get(license_url)
        obj = response.json()
        license_id = obj['fields']['titolo']['value'].lower()

        cache[license_url] = license_id

        return license_id

    def _comunweb_dataset_to_ckan(self, obj):
        """
        Prepare an object from comunweb for insertion to ckan
        """

        metadata = obj['full_metadata']
        values = comunweb_normalize_field_values(metadata)

        # License is available in:
        # metadata['fields']['licenza']['value']['link']
        # Downloading that URL gets to a json file; license id is in:
        # data['fields']['titolo']['value']
        license_url = metadata['fields']['licenza']['value']['link']
        license_id = self._get_cached_license(license_url)

        dataset = {
            'name': slugify(obj['objectName']),
            'title': obj['objectName'],
            'notes': '',
            'author': '',
            'author_email': '',
            'maintainer': '',
            'maintainer_email': '',
            'url': '',
            'license_id': license_id,
            'owner_org': self.conf['org_name'],
            'groups': [],  # todo: set this?
            'extras': {},
            'tags': [],
        }

        # Set dataset extras
        for key, val in values.iteritems():
            if val['value'] is not None:
                dataset['extras'][val['label']] = val['value']
                if isinstance(val['value'], datetime.datetime):
                    val['value'] = val['value'].strftime(DATE_FORMAT)

        return dataset


def comunweb_normalize_field_values(obj):
    def _get_field_value(fld):
        if fld['type'] == 'ezxmltext':
            # todo: strip tags, convert HTML entities
            return fld['string_value'] or None

        if fld['type'] == 'ezstring':
            return fld['value']  # should be == string_value

        if fld['type'] == 'ezkeyword':
            # This is a "tags" field
            # todo: should tags be splitted somehow?
            return fld['value'] or None

        if fld['type'] == 'ezdate':
            val = int(fld['value'])
            if val == 0:
                return None  # 0 is used as empty value

            # Note: we have no idea of which timezone has been used
            # to generate those timestamps -- let's assume UTC.
            # Otherwise, we should use pytz to read timestamp
            # and export it to the desired timezone.

            return datetime.datetime.utcfromtimestamp(val)

        if fld['type'] in ('ezbinaryfile', 'ezimage', 'ezurl'):
            # This is a URL pointing to something..
            return fld['value'] or None

        if fld['type'] == 'ezobjectrelationlist':
            # This is a relationship to some object
            # Value is something like this:

            # {
            # "classIdentifier": "tipo_formato",
            # "dateModified": 1362067615,
            # "datePublished": 1354868182,
            # "link": "http://.../api/opendata/v1/content/object/715852",
            # "objectId": 715852,
            # "objectName": "CSV (comma-separated values)",
            # "objectRemoteId": "356f4d6e6a1dd8d72ecfa2d12b061fcb"
            # }
            return None  # Not handled (yet)

        logger.warning('Encountered an unsupported field type: {0}'
                       .format(fld['type']))
        logger.debug('Field definition was: {0!r}'.format(fld))

    data = {}
    for fldname, fld in obj['fields'].iteritems():
        assert fld['identifier'] == fldname
        value = _get_field_value(fld)
        data[fldname] = {'value': value, 'label': fld['name']}
    return data
