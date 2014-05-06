# -*- coding: utf-8 -*-

import logging

import lxml.etree

from harvester.ext.converters.base import ConverterPluginBase
from harvester.utils import slugify, normalize_case

logger = logging.getLogger(__name__)


DEFAULT_ORG_ID = 'pat-sistema-informativo-ambiente-e-territorio'
LICENSES_MAP = {
    1: 'cc-zero',
    2: 'cc-by',
}


class GeoCatalogoToCkan(ConverterPluginBase):

    def convert(self, storage_in, storage_out):
        logger.info('Converting datasets PAT Geocatalogo -> ckan')

        _num_datasets = len(storage_in.documents['dataset'])
        all_datasets = storage_in.documents['dataset'].iteritems()
        for i, (dataset_id, dataset) in enumerate(all_datasets):
            logger.info('Converting dataset {0} [{1}/{2}]'
                        .format(dataset_id, i + 1, _num_datasets))

            # We load the XML from the dataset ``raw_xml`` field,
            # then we can extract relevant information.

            xml_obj = lxml.etree.fromstring(dataset['raw_xml'])
            converted = dataset_geocatalogo_to_ckan(xml_obj)
            assert int(converted['id']) == dataset_id
            storage_out.documents['dataset'][str(converted['id'])] = converted

        logger.info('Importing organizations')
        storage_out.documents['organization'][DEFAULT_ORG_ID] = {
            'name': DEFAULT_ORG_ID,
            'title': 'PAT Geocatalogo',
            'description': 'Geocatalogo',
            'image_url': 'http://dati.trentino.it/images/logo.png',
            'type': 'organization',
            'is_organization': True,
            'state': 'active',
            'tags': [],
        }

        # todo: import groups too..


def dataset_geocatalogo_to_ckan(dataset_xml):
    """
    We need to convert a dataset from the geocatalogo
    into a Ckan dataset.

    Beware that we actually need to parse *two* files in order
    to extract the required information.
    """

    ckan_dataset = {
        'id': None,  # the original one
        'name': None,
        'title': None,
        'notes': None,
        'author': None,
        'author_email': None,
        'maintainer': None,
        'maintainer_email': None,
        'url': None,
        'license_id': 'cc-zero',  # Always cc-zero!
        'owner_org': DEFAULT_ORG_ID,
        'groups': [],
        'extras': {},
    }

    # XPath shortcut
    xp = lambda x: dataset_xml.xpath(x, namespaces=dataset_xml.nsmap)

    # Get the id (numeric)
    ckan_dataset['id'] = str(int(xp('geonet:info/id/text()')[0]))

    # todo: we need to convert title to proper casing!
    _ds_title = xp('dc:title/text()')[0]
    _ds_title = normalize_case(_ds_title)
    _ds_name = slugify(_ds_title)

    ckan_dataset['name'] = _ds_name
    ckan_dataset['title'] = _ds_title

    # _ds_categories = xp('dc:subject/text()')
    try:
        _ds_abstract = xp('dct:abstract/text()')[0]
    except IndexError:
        _ds_abstract = ''
    ckan_dataset['notes'] = _ds_abstract

    _url_ogd_xml = xp('geonet:info/ogd_xml/text()')[0]
    _url_ogd_zip = xp('geonet:info/ogd_zip/text()')[0]

    try:
        _url_ogd_rdf = xp('geonet:info/ogd_rdf/text()')[0]
    except:
        _url_ogd_rdf = None  # f** this

    # Set license (we need to map numerical ids to actual license ids)
    ds_license = xp('geonet:info/licenseType/text()')[0]
    ckan_dataset['license_id'] = LICENSES_MAP[int(ds_license)]

    # _ds_groups = xp('geonet:info/groups/record/name/text()')
    _ds_owner = xp('geonet:info/ownername/text()')[0]

    ckan_dataset['author'] = _ds_owner.lower().title()
    ckan_dataset['author_email'] = (_ds_owner + '@example.com').lower()
    ckan_dataset['maintainer'] = ckan_dataset['author']
    ckan_dataset['maintainer_email'] = ckan_dataset['author_email']

    ckan_dataset['url'] = _url_ogd_xml

    ckan_dataset['extras'] = {
        'Data di creazione':
        xp('geonet:info/createDate/text()')[0],
        'Data di aggiornamento':
        xp('geonet:info/changeDate/text()')[0],

        'Copertura Geografica': 'Provincia di Trento',
        'Aggiornamento': 'Non programmato',
        'Codifica Caratteri': 'utf-8',
        'URL sito': 'http://www.territorio.provincia.tn.it',
    }

    ckan_dataset['resources'] = []

    if _url_ogd_xml:
        ckan_dataset['resources'].append({
            'name': _ds_title,
            'description': _ds_title,
            'format': 'XML',
            'mimetype': 'application/xml',
            'url': _url_ogd_xml,
            })
    if _url_ogd_zip:
        ckan_dataset['resources'].append({
            'name': _ds_title,
            'description': _ds_title,
            'format': 'ZIP',
            'mimetype': 'application/zip',
            'url': _url_ogd_zip,
        })
    if _url_ogd_rdf:
        ckan_dataset['resources'].append({
            'name': _ds_title,
            'description': _ds_title,
            'format': 'RDF',
            'mimetype': 'application/rdf+xml',
            'url': _url_ogd_rdf,
        })

    return ckan_dataset
