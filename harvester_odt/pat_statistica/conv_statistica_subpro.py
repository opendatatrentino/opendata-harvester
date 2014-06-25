# -*- coding: utf-8 -*-

"""
Functions to convert data from statistica sub-pro to ckan
"""

import logging

from harvester.utils import slugify
from .constants import LEGEND_TIPO_INDICATORE, CATEGORIES_MAP


logger = logging.getLogger(__name__)


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
            'Codifica Caratteri': 'UTF-8',
            'Titolare': 'Provincia Autonoma di Trento',
        },

        'resources': [],
    }

    # ------------------------------------------------------------
    # Populate extras

    if orig_dataset.get('AnnoInizio'):
        new_dataset['extras']['Copertura Temporale (Data di inizio)'] = \
            '{0}-01-01T00:00:00'.format(orig_dataset['AnnoInizio']),

    _fields = [
        (u"Aggiornamento", "FrequenzaAggiornamento"),
        (u"Ultimo aggiornamento", "UltimoAggiornamento"),
        (u"Algoritmo", "Algoritmo"),
        (u"Anno di base", "AnnoBase"),
        (u"Anno di inizio", "AnnoInizio"),
        (u"Area", "Area"),
        (u"Fonte", "Fonte"),
        (u"Frequenza di aggiornamento", "FrequenzaAggiornamento"),
        (u"Gruppo", "Gruppo"),
        (u"Livello Geografico Minimo", "LivelloGeograficoMinimo"),
        (u"Settore", "Settore"),
        (u"Tipo di Fenomeno", "TipoFenomento"),
        (u"Tipo di Fenomeno", "TipoFenomeno"),
        (u"Tipo di Indicatore", "TipoIndicatore"),
        (u"Unità di misura", u"UnitàMisura"),
    ]
    for _label, _orig in _fields:
        _value = orig_dataset.get(_orig)
        if _value:
            new_dataset['extras'][_label] = _value

    # ------------------------------------------------------------
    # Add resources

    # The main resources share title with the dataset.
    new_dataset['resources'] = [
        {
            'name': dataset_name,
            'description': dataset_title,
            'format': 'JSON',
            'mimetype': 'application/json',
            'url': orig_dataset['URLIndicatore'],
        },
        {
            'name': dataset_name,
            'description': dataset_title,
            'format': 'CSV',
            'mimetype': 'text/csv',
            'url': (orig_dataset['URLIndicatore']
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
