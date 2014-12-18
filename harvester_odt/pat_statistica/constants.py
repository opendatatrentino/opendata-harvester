# -*- coding: utf-8 -*-

import re

from unidecode import unidecode


LEGEND_TIPO_INDICATORE = {
    'R': 'rapporto',
    'M': 'media',
    'I': "incremento rispetto all'anno precedente",
}

# ======================================================================
#     IMPORTANT NOTE if you need to modify the rules below
# ======================================================================
# Category names are "cleaned up" before matching with the rules below:
# - Accented letters will be transliterated (eg. à becomes a, ...)
# - Any group of symbols (anything but letters and digits) will be
#   replaced with a single space
# - The whole string will be converted to lowercase
#
#              Keep that in mind when writing rules below!
# ======================================================================

CATEGORIES_MAP = {
    "agricoltura silvicoltura e pesca": "agricoltura",
    "ambiente": "ambiente",
    "altri servizi": "economia",
    "assistenza e protezione sociale": "welfare",
    "benessere economico": "welfare",
    "benessere soggettivo": "welfare",
    "commercio": "economia",
    "commercio con l estero e internazionalizzazione": "economia",
    "conti economici": "economia",
    "costruzioni": "economia",
    "credito e servizi finanziari": "economia",
    "cultura sport e tempo libero": "cultura",
    "edilizia e opere pubbliche": "gestione-del-territorio",
    "energia": "ambiente",
    "famiglia e comportamenti sociali": "welfare",
    "giustizia e sicurezza": "sicurezza",
    "industria": "economia",
    "istruzione e formazione": "conoscenza",
    "lavoro e conciliazione dei tempi di vita": "welfare",
    "mercato del lavoro": "welfare",
    "paesaggio e patrimonio culturale": "cultura",
    "politica e istituzioni": "politica",
    "popolazione": "demografia",
    "prezzi": "economia",
    "pubblica amministrazione": "amministrazione",
    "qualita dei servizi": "amministrazione",
    "relazioni sociali": "welfare",
    "ricerca e innovazione": "conoscenza",
    "ricerca sviluppo e innovazione": "conoscenza",
    "salute": "welfare",
    "sicurezza": "sicurezza",
    "societa dell informazione": "conoscenza",
    "stato dell ambiente": "ambiente",
    "struttura e competitivita delle imprese": "economia",
    "territorio": "gestione-del-territorio",
    "trasporti": "mobilita",
    "turismo": "turismo",
}

CATEGORIES = {
    'agricoltura': {'title': 'Agricoltura'},
    'ambiente': {'title': 'Ambiente'},
    'amministrazione': {'title': 'Amministrazione'},
    'conoscenza': {'title': 'Conoscenza'},
    'cultura': {'title': 'Cultura'},
    'demografia': {'title': 'Demografia'},
    'economia': {'title': 'Economia'},
    'gestione-del-territorio': {'title': 'Gestione del territorio'},
    'mobilita': {'title': 'Mobilità'},
    'politica': {'title': 'Politica'},
    'sicurezza': {'title': 'Sicurezza'},
    'turismo': {'title': 'Turismo'},
    'welfare': {'title': 'Welfare'},
}
for key, val in CATEGORIES.iteritems():
    val['name'] = key
    val['description'] = ''
    val['image_url'] = ''

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


def clean_category_name(name):
    transliterated = unidecode(name)
    cleaned = re.sub('[^a-zA-Z0-9]+', ' ', transliterated)
    cleaned = cleaned.strip().lower()
    return cleaned


def get_ckan_category(name):
    name = clean_category_name(name)
    return CATEGORIES_MAP.get(name)
