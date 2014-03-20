# -*- coding: utf-8 -*-

"""
API client for statweb.provincia.tn.it
"""

# todo: we should make this a two-step thing, downloading data in a
#       temporary db and cleaning it up later..

import json
import logging
import re
import hashlib

import requests


logger = logging.getLogger(__name__)


##----------------------------------------------------------------------
## Utilities
##----------------------------------------------------------------------

def _slugify(text):
    return re.sub(r'[^a-z0-9]+', '-', text.lower()).strip('-')


def _json_decode(text):
    text = text.replace('\n', ' ').replace('\r', '')
    return json.loads(text)


def _robohash(text):
    h = hashlib.sha1(text).hexdigest()
    return 'http://robohash.org/{0}.png?set=set1&bgset=bg1'.format(h)


##----------------------------------------------------------------------
## Constants
##----------------------------------------------------------------------

DEFAULT_BASE_URL = \
    'http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx'
DEFAULT_BASE_URL_SUBPRO = \
    'http://www.statweb.provincia.tn.it/indicatoristrutturalisubpro/exp.aspx'

## name : data
CATEGORIES = {
    'ambiente': {'title': 'Ambiente'},
    'amministrazione': {'title': 'Amministrazione'},
    'conoscenza': {'title': 'Conoscenza'},
    'cultura': {'title': 'Cultura'},
    'demografia': {'title': 'Demografia'},
    'economia': {'title': 'Economia'},
    'gestione-del-territorio': {'title': 'Gestione del territorio'},
    'politica': {'title': 'Politica'},
    'sicurezza': {'title': 'Sicurezza'},
    'welfare': {'title': 'Welfare'},
}
for key, val in CATEGORIES.iteritems():
    val['name'] = key
    val['description'] = ''  # todo: we need to preserve the original one!
    val['image_url'] = _robohash(key)  # PHUN!

## 'source -> ckan' map
CATEGORIES_MAP = {
    "Agricoltura, silvicoltura, e pesca": "economia",
    "Ambiente": "ambiente",
    "Altri servizi": "economia",
    "Assistenza e protezione sociale": "welfare",
    "Benessere economico": "welfare",
    "Benessere soggettivo": "welfare",
    "Commercio": "economia",
    "Commercio con l'estero e internazionalizzazione": "economia",
    "Conti economici": "economia",
    "Costruzioni": "economia",
    "Credito e servizi finanziari": "economia",
    "Cultura, sport e tempo libero": "cultura",
    "Edilizia e opere pubbliche": "ambiente",
    "Energia": "ambiente",
    "Famiglia e comportamenti sociali": "welfare",
    "Giustizia e sicurezza": "sicurezza",
    "Industria": "economia",
    "Istruzione e formazione": "conoscenza",
    "Lavoro e conciliazione dei tempi di vita": "welfare",
    "Mercato del lavoro": "welfare",
    "Paesaggio e patrimonio culturale": "ambiente",
    "Politica e istituzioni": "politica",
    "Popolazione": "demografia",
    "Prezzi": "economia",
    "Pubblica amministrazione": "amministrazione",
    "Qualità dei servizi": "welfare",
    "Relazioni sociali": "welfare",
    "Ricerca e innovazione": "conoscenza",
    "Ricerca, Sviluppo e innovazione": "conoscenza",
    "Salute": "welfare",
    "Sicurezza": "sicurezza",
    "Società dell'informazione": "conoscenza",
    "Stato dell'ambiente": "ambiente",
    "Struttura e competitività delle imprese": "economia",
    "Territorio": "gestione-del-territorio",
    "Trasporti": "economia",
    "Turismo": "economia",
}

## Organizations
ORGANIZATIONS = {
    'pat-s-statistica': {
        'name': 'pat-s-statistica',
        'title': 'PAT S. Statistica',
        'description':
        'Censimenti, analisi, indagine statistiche, indicatori, ...',
        'image_url': 'http://dati.trentino.it/images/logo.png',
        'type': 'organization',
        'is_organization': True,
        'state': 'active',
        'tags': [],
    }
}
for key, val in ORGANIZATIONS.iteritems():
    val['image_url'] = _robohash(key)  # PHUN!


##----------------------------------------------------------------------
## Clients
##----------------------------------------------------------------------

class StatisticaClient(object):
    """
    Client for statweb.provincia.tn.it "indicatori strutturali" API
    """

    default_base_url = DEFAULT_BASE_URL

    def __init__(self, base_url=None):
        self._base_url = base_url or self.default_base_url

    def index_url(self):
        return self._base_url + '?fmt=json'

    def metadata_url(self, id):
        return "{base}?fmt=json&idind={id}".format(
            base=self._base_url, id=id)

    def list_datasets(self):
        response = requests.get(self.index_url())
        assert response.ok
        data = _json_decode(response.text)
        assert isinstance(data, dict)
        assert data.keys() == ['IndicatoriStrutturali']

        return data['IndicatoriStrutturali']

    def iter_datasets(self, clean=True, suppress_exceptions=True):
        for record in self.list_datasets():
            try:
                yield self.get_dataset(record['id'], clean=clean)
            except:
                if not suppress_exceptions:
                    raise
                logger.exception('Failure retrieving dataset')

    def download_metadata(self, id):
        """
        Get raw metadata for a given dataset.
        """

        url = self.metadata_url(id)
        response = requests.get(url)
        assert response.ok
        data = _json_decode(response.text)

        assert isinstance(data, dict)
        assert len(data.keys()) == 1
        _ds_key = data.keys()[0]
        assert isinstance(data[_ds_key], list)
        assert len(data[_ds_key]) == 1
        orig_dataset = data[_ds_key][0]
        orig_dataset['id'] = id

        return orig_dataset

    def download_extended_metadata(self, id):
        """Download extended metadata for this dataset
        """

        dataset = self.download_metadata(id)

        ## Download linked resources, to extract metadata
        ##------------------------------------------------------------

        dataset['_links'] = {}
        dataset['_data'] = {}

        keys = [
            ('Indicatore', 'indicatore'),
            ('TabNumeratore', 'numeratore'),
            ('TabDenominatore', 'denominatore'),
        ]

        for orig, key in keys:
            if orig in dataset:
                url = dataset[orig]
                dataset['_links'][key] = url
                dataset['_data'][key] = requests.get(url).json()

        def _extract_title(json_data):
            assert isinstance(json_data, dict)
            assert len(json_data) == 1
            return json_data.keys()[0]

        ## Add resource titles, now that we have them
        ##------------------------------------------------------------

        dataset['title'] = _extract_title(dataset['_data']['indicatore'])
        dataset['name'] = _slugify(dataset['title'])

        for key in ('numeratore', 'denominatore'):
            if key in dataset['_data']:
                title = _extract_title(dataset['_data'][key])
                dataset[key + '_title'] = title
                dataset[key + '_name'] = _slugify(title)

        return dataset

    def get_dataset(self, id, clean=True):
        """Get cleaned metadata for a given dataset"""
        orig_dataset = self.download_extended_metadata(id)
        if clean:
            return dataset_statistica_to_ckan(orig_dataset)
        return orig_dataset


class StatisticaSubproClient(StatisticaClient):
    default_base_url = DEFAULT_BASE_URL_SUBPRO

    def index_url(self):
        return self._base_url + '?fmt=json&list=i'

    def metadata_url(self, id):  # todo: check this
        return "{base}?fmt=json&idind={id}".format(
            base=self._base_url, id=id)

    def list_datasets(self):  # todo: check this
        response = requests.get(self.index_url())
        assert response.ok

        data = _json_decode(response.text)
        assert isinstance(data, dict)
        assert data.keys() == ["Lista indicatori strutturali SP"]
        return data["Lista indicatori strutturali SP"]

    def iter_datasets(self, clean=True, suppress_exceptions=True):
        for record in self.list_datasets():
            ## Record already has all the information we need.
            ## Now, we just need to add extra data..
            try:
                record = self._add_extra_metadata(record)
                if clean:
                    record = dataset_statistica_subpro_to_ckan(record)
                yield record

            except:
                if not suppress_exceptions:
                    raise
                logger.exception('Failure retrieving dataset')

    def _add_extra_metadata(self, record):
        if 'URLTabNumMD' in record:
            response = requests.get(record['URLTabNumMD'])
            record['metadata_numeratore'] = response.json()

        if 'URLTabDenMD' in record:
            response = requests.get(record['URLTabDenMD'])
            record['metadata_denominatore'] = response.json()

        return record

    def download_metadata(self, id):
        raise NotImplementedError

    def download_extended_metadata(self, id):
        raise NotImplementedError

    def get_dataset(self, id, clean=True):
        raise NotImplementedError


##----------------------------------------------------------------------
## Conversion utilities
##----------------------------------------------------------------------

def dataset_statistica_to_ckan(orig_dataset):
    new_dataset = {
        'id': orig_dataset['id'],

        'name': orig_dataset['name'],
        'title': orig_dataset['title'],

        # 'notes' -> added later

        ## Fixed values
        'author': 'Servizio Statistica',
        'author_email': 'serv.statistica@provincia.tn.it',
        'maintainer': 'Servizio Statistica',
        'maintainer_email': 'serv.statistica@provincia.tn.it',

        ## Documentation
        'url': 'http://www.statweb.provincia.tn.it/INDICATORISTRUTTURALI'
        '/ElencoIndicatori.aspx',
        'license_id': 'cc-by',
        'owner_org': 'pat-s-statistica',  # org name

        'groups': [],  # group names -> should be populated from map

        'extras': {
            'Copertura Geografica': 'Provincia di Trento',
            'Copertura Temporale (Data di inizio)': '{0}-01-01T00:00:00'
            .format(orig_dataset['AnnoInizio']),
            ## UltimoAggiornamento -> data di fine
            'Aggiornamento': orig_dataset['FreqAggiornamento'],
            'Codifica Caratteri': 'UTF-8',
            'Titolare': 'Provincia Autonoma di Trento',

            ## Extra extras ----------------------------------------
            'Algoritmo': orig_dataset['Algoritmo'],
            'Anno inizio': orig_dataset['AnnoInizio'],
            'Confronti territoriali':
            orig_dataset['ConfrontiTerritoriali'],
            'Fenomeno': orig_dataset['Fenomeno'],
            'Measurement unit': orig_dataset['UM'],
        },

        'resources': [],
    }

    ##------------------------------------------------------------
    ## Add resources

    ind_title = orig_dataset['title']
    ind_name = orig_dataset['name']

    new_dataset['resources'] = [
        {
            'name': ind_name,
            'description': ind_title,
            'format': 'JSON',
            'mimetype': 'application/json',
            'url': orig_dataset['Indicatore'],
        },
        {
            'name': ind_name,
            'description': ind_title,
            'format': 'CSV',
            'mimetype': 'text/csv',
            'url': orig_dataset['IndicatoreCSV'],
        },
    ]

    ##------------------------------------------------------------
    ## Add resources for the "numeratore" table

    if 'numeratore_title' in orig_dataset:
        num_title = orig_dataset['numeratore_title']
        num_name = orig_dataset['numeratore_name']

        new_dataset['resources'].extend([
            {
                'name': num_name,
                'description': num_title,
                'format': 'JSON',
                'mimetype': 'application/json',
                'url': orig_dataset['TabNumeratore'],
            },
            {
                'name': num_name,
                'description': num_title,
                'format': 'CSV',
                'mimetype': 'text/csv',
                'url': orig_dataset['TabNumeratoreCSV'],
            },
        ])

    ##------------------------------------------------------------
    ## Add resources for the "denominatore" table

    if 'denominatore_title' in orig_dataset:
        den_title = orig_dataset['denominatore_title']
        den_name = orig_dataset['denominatore_name']

        new_dataset['resources'].extend([
            {
                'name': den_name,
                'description': den_title,
                'format': 'JSON',
                'mimetype': 'application/json',
                'url': orig_dataset['TabDenominatore'],
            },
            {
                'name': den_name,
                'description': den_title,
                'format': 'CSV',
                'mimetype': 'text/csv',
                'url': orig_dataset['TabDenominatoreCSV'],
            },
        ])

    ##------------------------------------------------------------
    ## Add description, aggregating value from some fields.

    description = []

    if orig_dataset.get('Area'):
        description.append(u'**Area:** {0}'
                           .format(orig_dataset['Area']))

    if orig_dataset.get('Settore'):
        description.append(u'**Settore:** {0}'
                           .format(orig_dataset['Settore']))

    if orig_dataset.get('Algoritmo'):
        description.append(u'**Algoritmo:** {0}'
                           .format(orig_dataset['Algoritmo']))

    if orig_dataset.get('UM'):
        description.append(u'**Unità di misura:** {0}'
                           .format(orig_dataset['UM']))

    if orig_dataset.get('Fenomeno'):
        description.append(u'**Fenomeno:** {0}'
                           .format(orig_dataset['Fenomeno']))

    if orig_dataset.get('ConfrontiTerritoriali'):
        description.append(u'**Confronti territoriali:** {0}'
                           .format(orig_dataset['ConfrontiTerritoriali']))

    if orig_dataset.get('Note'):
        description.append(u'**Note:** {0}'
                           .format(orig_dataset['Note']))

    new_dataset['notes'] = u'\n\n'.join(description)

    ##------------------------------------------------------------
    ## Add groups

    groups = []
    settore = orig_dataset['Settore']
    if settore in CATEGORIES_MAP:
        groups.append(CATEGORIES_MAP[settore])
    new_dataset['groups'] = groups

    ## todo: add tags from area + settore

    return new_dataset


def dataset_statistica_subpro_to_ckan(orig_dataset):
    raise NotImplementedError
