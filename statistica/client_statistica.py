"""
API client for statweb.provincia.tn.it
"""

import re

import requests

DEFAULT_BASE_URL = 'http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx'
INDEX_URL = DEFAULT_BASE_URL + "?fmt=json"
DATASET_URLS = {
    "metadata": DEFAULT_BASE_URL + '?fmt=json&idind={id}',
    "indicatore": DEFAULT_BASE_URL + '?fmt=json&idind={id}&t=i',
    "numeratore": DEFAULT_BASE_URL + '?fmt=json&idind={id}&t=n',
    "denominatore": DEFAULT_BASE_URL + '?fmt=json&idind={id}&t=d',
}


class StatisticaClient(object):
    def __init__(self, base_url=None):
        self._base_url = base_url or DEFAULT_BASE_URL

    def index_url(self):
        return self._base_url + '?fmt=json'

    def metadata_url(self, id):
        return "{base}?fmt=json&idind={id}".format(
            base=self._base_url, id=id)

    def indicatore_url(self, id):
        return "{base}?fmt=json&idind={id}&t=i".format(
            base=self._base_url, id=id)

    def numeratore_url(self, id):
        return "{base}?fmt=json&idind={id}&t=n".format(
            base=self._base_url, id=id)

    def denominatore_url(self, id):
        return "{base}?fmt=json&idind={id}&t=d".format(
            base=self._base_url, id=id)

    def list_datasets(self):
        response = requests.get(self.index_url())
        assert response.ok
        data = response.json()
        assert isinstance(data, dict)
        assert data.keys() == ['IndicatoriStrutturali']

        return data['IndicatoriStrutturali']

    def iter_datasets(self):
        for record in self.list_datasets():
            yield self.get_dataset_meta(record['id'])

    def get_dataset_meta(self, id):
        """Get metadata for a given dataset"""

        url = self.metadata_url(id)
        response = requests.get(url)
        assert response.ok
        data = response.json()

        assert isinstance(data, dict)
        assert len(data.keys()) == 1
        _ds_key = data.keys()[0]
        assert isinstance(data[_ds_key], list)
        assert len(data[_ds_key]) == 1
        orig_dataset = data[_ds_key][0]

        # xxx "Algoritmo",
        # xxx "AnnoInizio",
        #     "Area",
        #     "ConfrontiTerritoriali",
        # xxx "Descrizione",
        #     "Fenomeno",
        # xxx "FreqAggiornamento",
        #     "LivelloGeografico",
        # xxx "Note",
        #     "ProxDispDatoDefinitivo",
        #     "ProxDispDatoProvvisorio",
        #     "Settore",   -->   need map for groups!
        #     "UM",
        #     "UltimoAggiornamento"

        #     "Indicatore",
        #     "IndicatoreCSV",
        #     "TabDenominatore",
        #     "TabDenominatoreCSV",
        #     "TabNumeratore",
        #     "TabNumeratoreCSV",

        dataset_title = orig_dataset['Note']
        dataset_name = re.sub(r'[^a-z0-9]', '-', dataset_title.lower())

        new_dataset = {
            'id': id,

            'author': 'Servizio Statistica',
            'author_email': 'serv.statistica@provincia.tn.it',
            'license_id': 'cc-by',
            'maintainer': 'Servizio Statistica',
            'maintainer_email': 'serv.statistica@provincia.tn.it',
            'notes': orig_dataset['Note'],
            'owner_org': 'pat-s-statistica',  # org name
            'groups': [],  # group names
            'name': dataset_name,
            'title': orig_dataset['Descrizione'],

            'extras': {
                'Aggiornamento': orig_dataset['FreqAggiornamento'],
                'Codifica Caratteri': 'UTF-8',
                'Copertura Geografica': orig_dataset['Area'],

                'Copertura Temporale (Data di inizio)': '{0}-01-01T00:00:00'\
                .format(orig_dataset['AnnoInizio']),

                # 'Data di aggiornamento': '2013-05-28T00:00:00',
                # 'Data di pubblicazione': '2013-06-16T11:15:18.217230',
                'Titolare': 'Provincia Autonoma di Trento',

                ## More extras
                'algoritmo': orig_dataset['Algoritmo'],
                'anno_inizio': orig_dataset['AnnoInizio'],
                'confronti_territoriali': orig_dataset['ConfrontiTerritoriali'],
                'fenomeno': orig_dataset['Fenomeno'],
                'unita_misura': orig_dataset['UM'],
            },

            'resources': [],
        }

        ##------------------------------------------------------------
        ## Add resources

        ind_title = self._get_name(orig_dataset['Indicatore'])
        ind_name = _slugify(ind_title)

        new_dataset['title'] = ind_title
        new_dataset['name'] = ind_name[:100]  # MAX ALLOWED SIZE

        resources = [
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

        try:
            num_title = self._get_name(orig_dataset['TabNumeratore'])
        except:
            pass
        else:
            num_name = _slugify(num_title)
            resources.extend([
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

        try:
            den_title = self._get_name(orig_dataset['TabDenominatore'])
        except:
            pass
        else:
            den_name = _slugify(den_title)
            resources.extend([
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

        new_dataset['resources'] = resources

        return new_dataset

    def _get_name(self, url):
        response = requests.get(url)
        assert response.ok
        data = response.json()
        assert isinstance(data, dict)
        assert len(data) == 1
        return data.keys()[0]


def _slugify(text):
    return re.sub(r'[^a-z0-9]', '-', text.lower())
