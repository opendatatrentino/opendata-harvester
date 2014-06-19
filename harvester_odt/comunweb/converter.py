# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import HTMLParser
import datetime
import json
import logging
import os
import re

import requests

from harvester.ext.converter.base import ConverterPluginBase
from harvester.utils import slugify

logger = logging.getLogger(__name__)

DATE_FORMAT = '%F %T'

# Load extension -> mimetype definitions
HERE = os.path.dirname(__file__)
with open(os.path.join(HERE, 'mimetypes.json'), 'r') as fp:
    EXT_TO_MIME = json.load(fp)


def _html_unescape(s):
    return HTMLParser.HTMLParser().unescape(s)


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
            'title': obj['objectName'].strip(),
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
            'resources': [],
        }

        # Set dataset extras
        for key, val in values.iteritems():
            if val['value'] is not None:
                if isinstance(val['value'], datetime.datetime):
                    val['value'] = val['value'].strftime(DATE_FORMAT)
                dataset['extras'][val['label']] = val['value']

        # Update fields by reading from extras
        # ----------------------------------------

        try:
            dataset['notes'] = unicode(values['abstract']['value'] or '')
        except KeyError:
            pass

        try:
            dataset['notes'] += u'\n\n' + unicode(values['descrizione']['value'] or '')  # noqa
        except KeyError:
            pass

        extras = dataset['extras']

        try:
            extras['Copertura Temporale (Data di inizio)'] = \
                values['data_iniziovalidita']['value']
        except KeyError:
            pass

        try:
            extras['Copertura Temporale (Data di fine)'] = \
                values['data_fine_validita']['value']
        except KeyError:
            pass

        try:
            extras['Aggiornamento'] = \
                metadata['frequenza_aggiornamento']['value']['objectName']
        except KeyError:
            pass

        try:
            extras['Codifica Caratteri'] = extras.pop('Codifica caratteri')
        except KeyError:
            pass

        # Create resources
        # ----------------------------------------

        dataset['resources'] = list(self._resources_from_metadata(metadata))
        self._fix_resource_format_case(dataset['resources'])

        return dataset

    def _resources_from_metadata(self, metadata):
        """:param metadata:

        Metadata object. See the ``obj`` argument to
        ``comunweb_normalize_field_values()`` and the
        ``docs/open_data-node-*.json`` files for more information.
        """
        for x in self._resources_from_metadata_files(metadata['fields']):
            yield x
        for x in self._resources_from_metadata_urls(metadata['fields']):
            yield x

    def _resources_from_metadata_files(self, fields):
        for key, field in fields.iteritems():
            if field['type'] != 'ezbinaryfile':
                continue
            if not field['value']:
                # Could be ``False`` or an empty string
                continue

            url = field['value']

            resource = {
                'name': 'File da scaricare',
                'description': 'File scaricabile contenente i dati',
                'format': '',
                'mimetype': 'application/octet-stream',
                'url': url,
            }

            if '.' in url:
                urlext = field['value'].rsplit('.', 1)[-1]
            else:
                urlext = ''

            mime = EXT_TO_MIME.get(urlext.lower())
            if mime:
                resource['name'] = 'Dati in formato {0}'.format(urlext.upper())
                resource['mimetype'] = mime
                resource['format'] = urlext.upper()

            else:
                logger.warning(
                    'Unable to guess mime type from url: {0}'
                    .format(url))

            yield resource

    def _resources_from_metadata_urls(self, fields):
        for key, field in fields.iteritems():
            # Filter out uninteresting stuff
            if field['type'] != 'ezurl':
                continue
            if not field['value']:
                # Could be ``False`` or an empty string
                continue

            # We need to figure out the resource type from the stuff
            # in the URL value..
            if '|' in field['string_value']:
                url, url_title = field['value'].split('|', 1)
            else:
                url = field['value']
                url_title = ''

            resource = {
                'name': 'Link ai dati',
                'description': 'Collegamento esterno ai dati scaricabili',
                'format': '',
                'mimetype': 'application/octet-stream',
                'url': url,
            }

            m = re.match(r'^DOWNLOAD (?P<fmt>.*)$', url_title)
            if m:
                resource['format'] = fmt = m.group('fmt')
                resource['name'] = 'Dati in formato {0}'.format(fmt)
                mime = EXT_TO_MIME.get(fmt.lower())
                if mime is not None:
                    resource['mimetype'] = mime
            else:
                logger.warning(
                    'Unable to determine mimetype from label: {0}'
                    .format(url_title))

            yield resource

    def _fix_resource_format_case(self, resources):
        """
        Hack to set the case of the resource format.

        Ckan will convert certain formats to uppercase and certain
        others to lowercase, but the client will complain about this..

        - todo: apply a workaround for this in the client?
        - todo: make the client just issue warnings if updated dataset
                doesn't match? -> warnings.warn() + logger.warn()
        - todo: maybe that should be made configurable (in development
                we want to make sure we catch any inconsistencies!)
        """

        must_upper = ['CSV', 'KML', 'ZIP']
        for res in resources:
            if res['format'].upper() in must_upper:
                res['format'] = res['format'].upper()
            else:
                res['format'] = res['format'].lower()


def comunweb_normalize_field_values(obj):
    """
    Normalize values from a ComunWeb object.

    :param obj:
        A metadata object in the format returned by ComunWeb.
        Some examples can be found in ``docs/open_data-node-*.json``.

        Basically, it is a dict with a top-level key "fields" pointing
        to a dict whose keys are mapping to dicts describing the field.

        Example::

            "fields": {
                "abstract": {
                    "classattribute_id": 2643,
                    "description": "",
                    "id": 7657698,
                    "identifier": "abstract",
                    "name": "Abstract (descrizione breve)",
                    "string_value": "Personale in servizio al 31/12/13. ...",
                    "type": "ezxmltext",
                    "value": "<p>Personale in servizio al 31/12/13.</p> ..."
                },
            }

    :return: a dict like this::

        {
            'field_name': {
                'label': 'Field label',
                'value': 'Field value',
            }
        }
    """

    def _get_field_value(fld):
        if fld['type'] == 'ezxmltext':
            # We don't want HTML entities here, instead we restore
            # original characters and let ckan do the escaping for us.

            # Otherwise, we will end up with a lot of ``[HTML
            # Removed]`` garbage inside our texts..

            return _html_unescape(fld['string_value']) or None

        if fld['type'] == 'ezstring':
            if fld['value'] != fld['string_value']:
                logger.warning(
                    "String fields value doesn't match string_value")
                logger.debug(
                    "value={0!r} string_value={1!r}".format(
                        fld['value'], fld['string_value']))
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
