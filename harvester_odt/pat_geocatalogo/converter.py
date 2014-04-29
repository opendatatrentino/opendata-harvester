# -*- coding: utf-8 -*-

import logging

import lxml.etree

from harvester.ext.converters.base import ConverterPluginBase
from harvester.utils import slugify, normalize_case

logger = logging.getLogger(__name__)


class GeoCatalogoToCkan(ConverterPluginBase):

    def convert(self, storage_in, storage_out):
        logger.debug('Converting datasets PAT Geocatalogo -> ckan')

        for dataset_id, dataset in storage_in.documents['dataset'].iteritems():
            # We load the XML from the dataset ``raw_xml`` field,
            # then we can extract relevant information.

            xml_obj = lxml.etree.fromstring(dataset['raw_xml'])
            converted = dataset_geocatalogo_to_ckan(xml_obj)
            assert converted['id'] == dataset_id
            storage_out.documents['dataset'][converted['id']] = converted

        # todo: import organizations / groups


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
        'owner_org': ['geocatalogo'],  # Mh?
        'groups': [],
        'extras': {},
    }

    # XPath shortcut
    xp = lambda x: dataset_xml.xpath(x, namespaces=dataset_xml.nsmap)

    # Get the id (numeric)
    ckan_dataset['id'] = int(xp('geonet:info/id/text()')[0])

    # todo: we need to convert title to proper casing!
    _ds_title = xp('dc:title/text()')[0]
    _ds_title = normalize_case(_ds_title)
    _ds_name = slugify(_ds_title)

    ckan_dataset['name'] = _ds_name
    ckan_dataset['title'] = _ds_title

    # _ds_categories = xp('dc:subject/text()')
    _ds_abstract = xp('dc:abstract/text()')
    ckan_dataset['notes'] = _ds_abstract

    _url_ogd_xml = xp('geonet:info/ogd_xml/text()')[0]
    _url_ogd_zip = xp('geonet:info/ogd_zip/text()')[0]

    try:
        _url_ogd_rdf = xp('geonet:info/ogd_rdf/text()')[0]
    except:
        _url_ogd_rdf = None  # f** this

    # todo: we need to consider licenses!! -> where is the definition??
    # _ds_license = xp('geonet:info/licenseType/text()')[0]
    # if _ds_license != '1':
    #     raise ValueError("Invalid license: {0!r}".format(_ds_license))

    # _ds_groups = xp('geonet:info/groups/record/name/text()')
    _ds_owner = xp('geonet:info/ownername/text()')[0]

    ckan_dataset['author'] = _ds_owner.lower().title()
    ckan_dataset['author_email'] = (_ds_owner + '@example.com').lower()
    ckan_dataset['maintainer'] = ckan_dataset['author']
    ckan_dataset['maintainer_email'] = ckan_dataset['author_email']

    ckan_dataset['url'] = _url_ogd_xml

    ckan_dataset['extras'] = {
        'Copertura Temporale (Data di inizio)':
        xp('geonet:info/createDate/text()')[0],
        'Copertura Temporale (Data di fine)':
        xp('geonet:info/changeDate/text()')[0],
        'Aggiornamento': 'yes',
        'Codifica Caratteri': 'utf-8',
        'Confronti territoriali  - Copertura Geografica':
        'Provincia di Trento',
    }

    ckan_dataset['resources'] = [
        {
            'name': _ds_title,
            'description': _ds_title,
            'format': 'XML',
            'mimetype': 'application/xml',
            'url': _url_ogd_xml,
        },
        {
            'name': _ds_title,
            'description': _ds_title,
            'format': 'ZIP',
            'mimetype': 'application/zip',
            'url': _url_ogd_zip,
        },
        {
            'name': _ds_title,
            'description': _ds_title,
            'format': 'RDF',
            'mimetype': 'application/rdf+xml',
            'url': _url_ogd_rdf,
        },
    ]

    return ckan_dataset
