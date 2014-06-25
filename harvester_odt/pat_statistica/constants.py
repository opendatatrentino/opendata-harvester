# -*- coding: utf-8 -*-

LEGEND_TIPO_INDICATORE = {
    'R': 'rapporto',
    'M': 'media',
    'I': "incremento rispetto all'anno precedente",
}

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
