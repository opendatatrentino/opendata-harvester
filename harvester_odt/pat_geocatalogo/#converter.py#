import logging

import lxml

from harvester.ext.converters.base import ConverterPluginBase
from harvester.utils import slugify

logger = logging.getLogger(__name__)


class GeoCatalogoToCkan(ConverterPluginBase):

    def convert(self, storage_in, storage_out):
        logger.debug('Converting datasets PAT Geocatalogo -> ckan')

        for dataset in storage_in.iter_objects('dataset'):
            # We load the XML from the dataset ``raw_xml`` field,
            # then we can extract relevant information.

            xml_obj = lxml.etree.fromstring(dataset['raw_xml'])
            converted = dataset_geocatalogo_to_ckan(xml_obj)
            storage_out.set_object('dataset', converted['id'], converted)

        # self.logger.debug('Importing groups')
        # for group in CATEGORIES.itervalues():
        #     storage_out.set_object('group', group['name'], group)

        # self.logger.debug('Importing organizations')
        # for org in ORGANIZATIONS.itervalues():
        #     storage_out.set_object('organization', org['name'], org)


def dataset_geocatalogo_to_ckan(dataset_xml):
    # XPath shortcut
    xp = lambda x: dataset_xml.xpath(x, namespaces=dataset_xml.nsmap)

    _ds_id = int(xp('geonet:info/id/text()')[0])

    # todo: we need to convert title to proper casing!
    _ds_title = xp('geonet:info/title/text()')[0]
    _ds_name = slugify(_ds_title)

    _ds_categories = xp('dc:subject/text()')
    _ds_abstract = xp('dc:abstract/text()')

    _url_ogd_xml = xp('geonet:info/ogd_xml/text()')[0]
    _url_ogd_zip = xp('geonet:info/ogd_zip/text()')[0]
    _url_ogd_rdf = xp('geonet:info/ogd_rdf/text()')[0]
    _ds_license = xp('geonet:info/licenseType/text()')[0]
    _ds_groups = xp('geonet:info/groups/record/name/text()')
    _ds_owner = xp('geonet:info/ownername/text()')
    _ds_schema = xp('geonet:info/schema/text()')

    new_dataset = {
        'id': _ds_id,
        'name': _ds_name,
        'title': _ds_title,

        'notes': _ds_abstract,

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
        # {
        #     'name': dataset_name,
        #     'description': dataset_title,
        #     'format': 'JSON',
        #     'mimetype': 'application/json',
        #     'url': orig_dataset['URLIndicatoreD'],
        # }
    pass

    # todo: generate description / notes
    # todo: associate groups
    # todo: associate organization

    return new_dataset
