# -*- coding: utf-8 -*-

"""
API client for statweb.provincia.tn.it
"""

# todo: we should make this a two-step thing, downloading data in a
#       temporary db and cleaning it up later..

# todo: make robohash-powered images optional

import abc
import logging

import requests

from harvester.utils import slugify, decode_faulty_json


logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------

DEFAULT_BASE_URL = \
    'http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx'
DEFAULT_BASE_URL_SUBPRO = \
    'http://www.statweb.provincia.tn.it/indicatoristrutturalisubpro/exp.aspx'

# ----------------------------------------------------------------------
# Clients
# ----------------------------------------------------------------------


class StatisticaClientBase(object):
    __metaclass__ = abc.ABCMeta

    default_base_url = abc.abstractproperty(lambda self: None)
    index_page_main_key = abc.abstractproperty(lambda self: None)

    def __init__(self, base_url=None, brute_force=False):
        """
        :param base_url: Base URL for Ckan
        """
        self._base_url = base_url or self.default_base_url

    @abc.abstractmethod
    def index_url(self):
        pass

    @abc.abstractmethod
    def metadata_url(self, id):
        pass

    def list_datasets(self):  # todo: check this
        response = requests.get(self.index_url())
        assert response.ok
        data = decode_faulty_json(response.text)
        assert isinstance(data, dict)
        assert data.keys() == [self.index_page_main_key]
        return data[self.index_page_main_key]

    def iter_datasets(self, suppress_exceptions=True):
        for record in self.list_datasets():
            try:
                yield self.get_dataset(record['id'])
            except:
                if not suppress_exceptions:
                    raise
                logger.exception('Failure retrieving dataset')

    def force_iter_datasets(self):
        """Iterate datasets, then try guessing numbers"""

        found = set()
        for record in self.iter_datasets():
            found.add(int(record['id']))
            yield record

        # Let's try guessing numbers up to 20% more than the highest
        # id found in the list.
        stop = int(max(int(x) for x in found) * 1.2)
        for i in xrange(1, stop + 1):
            if i in found:
                # We already returned this one
                continue
            try:
                yield self.get_dataset(i)
            except:  # Simply ignore anything bad that would happen..
                logger.exception("Exception while trying to brute-force "
                                 "get dataset {0}".format(i))

    def get_dataset(self, id):
        url = self.metadata_url(id)
        response = requests.get(url)
        assert response.ok
        data = decode_faulty_json(response.text)

        assert isinstance(data, dict)
        assert len(data.keys()) == 1
        _ds_key = data.keys()[0]
        assert isinstance(data[_ds_key], list)
        assert len(data[_ds_key]) == 1
        orig_dataset = data[_ds_key][0]
        orig_dataset['id'] = id

        return orig_dataset


class StatisticaClient(StatisticaClientBase):
    """
    Client for statweb.provincia.tn.it "indicatori strutturali" API
    """

    default_base_url = DEFAULT_BASE_URL
    index_page_main_key = 'IndicatoriStrutturali'

    def __init__(self, base_url=None, brute_force=False):
        """
        :param base_url: Base URL for Ckan
        """
        self._base_url = base_url or self.default_base_url

    def index_url(self):
        return self._base_url + '?fmt=json'

    def metadata_url(self, id):
        return "{base}?fmt=json&idind={id}".format(
            base=self._base_url, id=id)

    def get_dataset(self, id):
        dataset = super(StatisticaClient, self).get_dataset(id)
        self._add_extended_metadata(dataset)
        return dataset

    def _add_extended_metadata(self, dataset):
        """Download extended metadata for this dataset
        """

        # Download linked resources, to extract metadata
        # ------------------------------------------------------------

        dataset['_links'] = {}
        dataset_data = {}

        keys = [
            ('Indicatore', 'indicatore'),
            ('TabNumeratore', 'numeratore'),
            ('TabDenominatore', 'denominatore'),
        ]

        for orig, key in keys:
            if orig in dataset:
                url = dataset[orig]
                dataset['_links'][key] = url
                dataset_data[key] = requests.get(url).json()

        def _extract_title(json_data):
            assert isinstance(json_data, dict)
            assert len(json_data) == 1
            return json_data.keys()[0]

        # Add resource titles, now that we have them
        # ------------------------------------------------------------

        dataset['title'] = _extract_title(dataset_data['indicatore'])
        dataset['name'] = slugify(dataset['title'])

        for key in ('numeratore', 'denominatore'):
            if key in dataset_data:
                title = _extract_title(dataset_data[key])
                dataset[key + '_title'] = title
                dataset[key + '_name'] = slugify(title)

        return dataset


class StatisticaSubproClient(StatisticaClientBase):
    default_base_url = DEFAULT_BASE_URL_SUBPRO
    index_page_main_key = "Lista indicatori strutturali SP"

    def index_url(self):
        return self._base_url + '?fmt=json&list=i'

    def metadata_url(self, id):  # todo: check this
        return "{base}?fmt=json&idind={id}&info=md".format(
            base=self._base_url, id=id)

    def iter_datasets(self, suppress_exceptions=True):
        for record in self.list_datasets():
            # Record already has all the information we need.
            # Now, we just need to add extra data..
            try:
                record = self._add_extended_metadata(record)
                yield record

            except:
                if not suppress_exceptions:
                    raise
                logger.exception('Failure retrieving dataset')

    def get_dataset(self, id):
        dataset = super(StatisticaSubproClient, self).get_dataset(id)
        self._add_extended_metadata(dataset)
        return dataset

    def _add_extended_metadata(self, record):
        # Note: we no longer need to download the whole thing
        # in order to get the title, as it is just in the
        # todo: we can cache sub-tables!

        if record.get('DescrizioneTabNum'):
            record['metadata_numeratore'] = {
                'title': record['DescrizioneTabNum'],
                'url': record.get('URLTabNumMD'),
            }

        if record.get('DescrizioneTabDen'):
            record['metadata_denominatore'] = {
                'title': record['DescrizioneTabDen'],
                'url': record.get('URLTabDenMD'),
            }

        return record
