"""
Functions to convert data from statistica to ckan
"""

import logging

from .constants import CATEGORIES_MAP


logger = logging.getLogger(__name__)


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
    # ind_name = orig_dataset['name']

    new_dataset['resources'] = [
        {
            'name': ind_title,
            'description': ind_title,
            'format': 'JSON',
            'mimetype': 'application/json',
            'url': orig_dataset['Indicatore'],
        },
        {
            'name': ind_title,
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
        # num_name = orig_dataset['numeratore_name']

        new_dataset['resources'].extend([
            {
                'name': num_title,
                'description': num_title,
                'format': 'JSON',
                'mimetype': 'application/json',
                'url': orig_dataset['TabNumeratore'],
            },
            {
                'name': num_title,
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
        # den_name = orig_dataset['denominatore_name']

        new_dataset['resources'].extend([
            {
                'name': den_title,
                'description': den_title,
                'format': 'JSON',
                'mimetype': 'application/json',
                'url': orig_dataset['TabDenominatore'],
            },
            {
                'name': den_title,
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
