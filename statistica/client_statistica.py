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
INDEX_URL = DEFAULT_BASE_URL + "?fmt=json"
DATASET_URLS = {
    "metadata": DEFAULT_BASE_URL + '?fmt=json&idind={id}',
    "indicatore": DEFAULT_BASE_URL + '?fmt=json&idind={id}&t=i',
    "numeratore": DEFAULT_BASE_URL + '?fmt=json&idind={id}&t=n',
    "denominatore": DEFAULT_BASE_URL + '?fmt=json&idind={id}&t=d',
}

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


##----------------------------------------------------------------------
## Clients
##----------------------------------------------------------------------

class StatisticaClient(object):
    """
    Client for statweb.provincia.tn.it "indicatori strutturali" API
    """

    def __init__(self, base_url=None):
        self._base_url = base_url or DEFAULT_BASE_URL

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

    def iter_datasets(self, suppress_exceptions=True):
        for record in self.list_datasets():
            try:
                yield self.get_dataset_meta(record['id'])
            except:
                if not suppress_exceptions:
                    raise
                logger.exception('Failure retrieving dataset')

    def iter_raw_datasets(self, suppress_exceptions=True):
        for record in self.list_datasets():
            try:
                yield self.get_raw_dataset_meta(record['id'])
            except:
                if not suppress_exceptions:
                    raise
                logger.exception('Failure retrieving dataset')

    def get_raw_dataset_meta(self, id):
        """Get raw metadata for a given dataset"""

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

    def get_dataset_meta(self, id):
        """Get cleaned metadata for a given dataset"""

        orig_dataset = self.get_raw_dataset_meta(id)
        return self.source_dataset_to_ckan(orig_dataset)

    def source_dataset_to_ckan(self, orig_dataset):
        new_dataset = {
            'id': orig_dataset['id'],

            # 'name' -> added later
            # 'title' -> added later

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

            # 'notes' -> added later

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
                'algoritmo': orig_dataset['Algoritmo'],
                'anno_inizio': orig_dataset['AnnoInizio'],
                'confronti_territoriali':
                orig_dataset['ConfrontiTerritoriali'],
                'fenomeno': orig_dataset['Fenomeno'],

                'measurement_unit': orig_dataset['UM'],
            },

            'resources': [],
        }

        ##------------------------------------------------------------
        ## Add resources

        ind_title = self._get_name(orig_dataset['Indicatore'])
        ind_name = _slugify(ind_title)

        ## We need the indicatore title in order to build title / name
        new_dataset['title'] = ind_title

        ## Maximum length for the name is 100 characters, but it is not
        ## a good idea to have too-long urls..
        new_dataset['name'] = ind_name[:60]

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

        num_url = orig_dataset.get('TabNumeratore')
        if num_url:
            num_title = self._get_name(num_url)
            num_name = _slugify(num_title)
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

        den_url = orig_dataset.get('TabDenominatore')
        if den_url:
            den_title = self._get_name(den_url)
            den_name = _slugify(den_title)
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

        ## todo: add tags

        return new_dataset

    def _get_name(self, url):
        """
        Get name from a json files at a URL.

        It will:

        - download the json data
        - make sure it is a one-key dictionary
        - return the value of the one key
        """

        response = requests.get(url)
        assert response.ok
        data = _json_decode(response.text)
        assert isinstance(data, dict)
        assert len(data) == 1
        return data.keys()[0]
