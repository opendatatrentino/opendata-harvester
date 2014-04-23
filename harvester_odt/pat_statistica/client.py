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

from harvester.utils import slugify, decode_faulty_json, get_robohash_url


logger = logging.getLogger(__name__)

ADD_ROBOHASH = False


# ----------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------


def _clean_metadata_dict_for_subpro_num_den(dct):
    """
    Clean metadata dictionary for numeratore/denominatore in
    "indicatori sub-provinciali", checking that the passed-in
    format is consistent with the expected one..

    >>> _clean_metadata_dict_for_subpro_num_den({
    ...   'Title' : [{
    ...     'hello': 'world',
    ...   }]
    ... })
    ('Title', {'hello': 'world'})
    """
    if not isinstance(dct, dict):
        raise TypeError("Expected a dict")
    if len(dct) != 1:
        raise ValueError("Expected a single-key dict")
    title = dct.keys()[0]
    if not isinstance(dct[title], list):
        raise ValueError("Expected a single-key dict containing a list")
    if len(dct[title]) != 1:
        raise ValueError(
            "Expected a single-key dict containing a single-item list")
    return title, dct[title][0]


# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------

DEFAULT_BASE_URL = \
    'http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx'
DEFAULT_BASE_URL_SUBPRO = \
    'http://www.statweb.provincia.tn.it/indicatoristrutturalisubpro/exp.aspx'

# name : data
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
    if ADD_ROBOHASH:
        val['image_url'] = get_robohash_url(key)  # PHUN!

# 'source -> ckan' map
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

# Organizations
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
if ADD_ROBOHASH:
    for key, val in ORGANIZATIONS.iteritems():
        val['image_url'] = get_robohash_url(key)  # PHUN!

LEGEND_TIPO_INDICATORE = {
    'R': 'rapporto',
    'M': 'media',
    'I': "incremento rispetto all'anno precedente",
}


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
        # todo: we can cache sub-tables!

        if record.get('URLTabNumMD'):  # non empty!
            response = requests.get(record['URLTabNumMD'])
            record['metadata_numeratore'] = response.json()

        if record.get('URLTabDenMD'):  # non empty!
            response = requests.get(record['URLTabDenMD'])
            record['metadata_denominatore'] = response.json()

        return record

    # def download_metadata(self, id):
    #     raise NotImplementedError

    # def download_extended_metadata(self, id):
    #     raise NotImplementedError


# ----------------------------------------------------------------------
# Conversion utilities
# ----------------------------------------------------------------------

def dataset_statistica_to_ckan(orig_dataset):
    new_dataset = {
        'id': orig_dataset['id'],

        'name': orig_dataset['name'],
        'title': orig_dataset['title'],

        # 'notes' -> added later

        # Fixed values
        'author': 'Servizio Statistica',
        'author_email': 'serv.statistica@provincia.tn.it',
        'maintainer': 'Servizio Statistica',
        'maintainer_email': 'serv.statistica@provincia.tn.it',

        # Documentation
        'url': 'http://www.statweb.provincia.tn.it/INDICATORISTRUTTURALI'
        '/ElencoIndicatori.aspx',
        'license_id': 'cc-by',
        'owner_org': 'pat-s-statistica',  # org name

        'groups': [],  # group names -> should be populated from map

        'extras': {
            'Copertura Geografica': 'Provincia di Trento',
            'Copertura Temporale (Data di inizio)': '{0}-01-01T00:00:00'
            .format(orig_dataset['AnnoInizio']),
            # UltimoAggiornamento -> data di fine
            'Aggiornamento': orig_dataset['FreqAggiornamento'],
            'Codifica Caratteri': 'UTF-8',
            'Titolare': 'Provincia Autonoma di Trento',

            # Extra extras ----------------------------------------
            'Algoritmo': orig_dataset['Algoritmo'],
            'Anno inizio': orig_dataset['AnnoInizio'],
            'Confronti territoriali':
            orig_dataset['ConfrontiTerritoriali'],
            'Fenomeno': orig_dataset['Fenomeno'],
            'Measurement unit': orig_dataset['UM'],
        },

        'resources': [],
    }

    # ------------------------------------------------------------
    # Add resources

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

    # ------------------------------------------------------------
    # Add resources for the "numeratore" table

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

    # ------------------------------------------------------------
    # Add resources for the "denominatore" table

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

    # ------------------------------------------------------------
    # Add description, aggregating value from some fields.

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

    # ------------------------------------------------------------
    # Add groups

    groups = []
    settore = orig_dataset['Settore']
    if settore in CATEGORIES_MAP:
        groups.append(CATEGORIES_MAP[settore])
    new_dataset['groups'] = groups

    # todo: add tags from area + settore

    return new_dataset


def dataset_statistica_subpro_to_ckan(orig_dataset):
    dataset_title = orig_dataset['Descrizione']
    dataset_name = slugify(dataset_title)

    new_dataset = {
        'id': orig_dataset['id'],
        'name': dataset_name,
        'title': dataset_title,

        # 'notes' -> added later

        # Fixed values
        'author': 'Servizio Statistica',
        'author_email': 'serv.statistica@provincia.tn.it',
        'maintainer': 'Servizio Statistica',
        'maintainer_email': 'serv.statistica@provincia.tn.it',

        # Documentation
        'url': 'http://www.statweb.provincia.tn.it/'
        'INDICATORISTRUTTURALISubPro/',
        'license_id': 'cc-by',
        'owner_org': 'pat-s-statistica',  # org name

        'groups': [],  # group names -> should be populated from map

        'extras': {
            'Copertura Geografica': 'Provincia di Trento',
            'Copertura Temporale (Data di inizio)': '{0}-01-01T00:00:00'
            .format(orig_dataset['AnnoInizio']),
            # UltimoAggiornamento -> data di fine
            'Aggiornamento': orig_dataset['FrequenzaAggiornamento'],
            "Ultimo aggiornamento": orig_dataset["UltimoAggiornamento"],
            'Codifica Caratteri': 'UTF-8',
            'Titolare': 'Provincia Autonoma di Trento',

            # Extra extras ----------------------------------------
            u"Algoritmo": orig_dataset["Algoritmo"],
            u"Anno di base": orig_dataset["AnnoBase"],
            u"Anno di inizio": orig_dataset["AnnoInizio"],
            u"Area": orig_dataset["Area"],
            # "Descrizione": orig_dataset["Descrizione"],
            # "DescrizioneEstesa": orig_dataset["DescrizioneEstesa"],
            # "DescrizioneTabDen": orig_dataset["DescrizioneTabDen"],
            # "DescrizioneTabNum": orig_dataset["DescrizioneTabNum"],
            "Fonte": orig_dataset["Fonte"],
            "Frequenza di aggiornamento":
            orig_dataset["FrequenzaAggiornamento"],
            "Gruppo": orig_dataset["Gruppo"],
            "Livello Geografico Minimo":
            orig_dataset["LivelloGeograficoMinimo"],
            # "NomeTabDen": orig_dataset["NomeTabDen"],
            # "NomeTabNum": orig_dataset["NomeTabNum"],
            "Settore": orig_dataset["Settore"],
            "Tipo di Fenomeno": orig_dataset["TipoFenomento"],  # mind the typo
            "Tipo di Indicatore": orig_dataset["TipoIndicatore"],
            # "URLIndicatoreD": orig_dataset["URLIndicatoreD"],
            # "URLTabDenMD": orig_dataset["URLTabDenMD"],
            # "URLTabNumMD": orig_dataset["URLTabNumMD"],
            u"Unità di misura": orig_dataset[u"UnitàMisura"],
        },

        'resources': [],
    }

    # ------------------------------------------------------------
    # Add resources

    # The main resources share title with the dataset.

    new_dataset['resources'] = [
        {
            'name': dataset_name,
            'description': dataset_title,
            'format': 'JSON',
            'mimetype': 'application/json',
            'url': orig_dataset['URLIndicatoreD'],
        },
        {
            'name': dataset_name,
            'description': dataset_title,
            'format': 'CSV',
            'mimetype': 'text/csv',
            'url': (orig_dataset['URLIndicatoreD']
                    .replace('fmt=json', 'fmt=csv')),  # F** this
        },
    ]

    # ------------------------------------------------------------
    # Add resources for the "numeratore" table

    if 'metadata_numeratore' in orig_dataset:
        _title, _md = _clean_metadata_dict_for_subpro_num_den(
            orig_dataset['metadata_numeratore'])

        num_title = _title
        num_name = slugify(_title)

        new_dataset['resources'].extend([
            {
                'name': num_name,
                'description': num_title,
                'format': 'JSON',
                'mimetype': 'application/json',
                'url': _md['URLTabD'],
            },
            {
                'name': num_name,
                'description': num_title,
                'format': 'CSV',
                'mimetype': 'text/csv',
                'url': (_md['URLTabD']
                        .replace('fmt=json', 'fmt=csv')),  # F** this
            },
        ])

    # ------------------------------------------------------------
    # Add resources for the "denominatore" table

    if 'metadata_denominatore' in orig_dataset:
        _title, _md = _clean_metadata_dict_for_subpro_num_den(
            orig_dataset['metadata_denominatore'])

        den_title = _title
        den_name = slugify(_title)

        new_dataset['resources'].extend([
            {
                'name': den_name,
                'description': den_title,
                'format': 'JSON',
                'mimetype': 'application/json',
                'url': _md['URLTabD'],
            },
            {
                'name': den_name,
                'description': den_title,
                'format': 'CSV',
                'mimetype': 'text/csv',
                'url': (_md['URLTabD']
                        .replace('fmt=json', 'fmt=csv')),  # F** this
            },
        ])

    # ------------------------------------------------------------
    # Add description, aggregating value from some fields.

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

    if orig_dataset.get('UnitàMisura'):
        description.append(u'**Unità di misura:** {0}'
                           .format(orig_dataset['UnitàMisura']))

    if orig_dataset.get('TipoFenomeno'):
        description.append(u'**Fenomeno:** {0}'
                           .format(orig_dataset['TipoFenomeno']))

    if orig_dataset.get('TipoIndicatore'):
        _orig_value = orig_dataset['TipoIndicatore']
        _value = LEGEND_TIPO_INDICATORE.get(_orig_value)
        if _value is None:
            _value = 'unknown ({0!r})'.format(_orig_value)
        description.append(u'**Tipo di indicatore:** {0}'.format(_value))
        del _orig_value, _value  # clean garbage..

    if orig_dataset.get('AnnoBase'):
        description.append(u'**Anno di base:** {0}'
                           .format(orig_dataset['AnnoBase']))

    if orig_dataset.get('ConfrontiTerritoriali'):
        description.append(u'**Confronti territoriali:** {0}'
                           .format(orig_dataset['ConfrontiTerritoriali']))

    if orig_dataset.get('LivelloGeograficoMinimo'):
        description.append(u'**Livello geografico minimo:** {0}'
                           .format(orig_dataset['LivelloGeograficoMinimo']))

    if orig_dataset.get('Note'):
        description.append(u'**Note:** {0}'
                           .format(orig_dataset['Note']))

    new_dataset['notes'] = u'\n\n'.join(description)

    # ------------------------------------------------------------
    # Add groups

    groups = []
    settore = orig_dataset['Settore']
    if settore in CATEGORIES_MAP:
        groups.append(CATEGORIES_MAP[settore])
    new_dataset['groups'] = groups

    # todo: add tags from area + settore

    return new_dataset
