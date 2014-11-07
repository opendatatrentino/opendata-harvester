# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import datetime
import logging

import lxml.etree

from harvester.ext.converter.base import ConverterPluginBase
from harvester.utils import slugify, XPathHelper, flatten_dict, report_progress

from .tags_map import TAGS_MAP
from .constants import LINKED_XML_NSMAP, API_XML_NSMAP

logger = logging.getLogger(__name__)


ORG_SIAT = 'pat-sistema-informativo-ambiente-e-territorio'
ORG_CATASTO = 'pat-s-catasto'
LICENSES_MAP = {
    1: 'cc-zero',
    2: 'cc-by',
}

ORGANIZATIONS = {
    ORG_SIAT: {
        'name': ORG_SIAT,
        'title': 'PAT Sistema Informativo Ambiente e Territorio',
        'description': 'SIAT. Entità territoriali geo-referenziate,'
        ' con associate informazioni sulle relative proprietà.',
        'image_url': 'http://dati.trentino.it/images/logo.png',
        'type': 'organization',
        'is_organization': True,
        'state': 'active',
        'tags': [],
    },
    ORG_CATASTO: {
        'name': ORG_CATASTO,
        'title': 'PAT S. Catasto',

        'description':
        "Il Servizio Catasto della Provincia autonoma di Trento cura le"
        "seguenti attività: - sovrintende alle operazioni di"
        "conservazione del catasto fondiario e fabbricati; - svolge le"
        "funzioni di controllo, di verifica e di ispezione delle attività"
        "connesse alla tenuta del catasto; - cura, in accordo con la"
        "struttura competente in materia di Sistema informativo"
        "elettronico provinciale, la definizione dei programmi di"
        "informatizzazione dei servizi del catasto nel contesto di una"
        "coordinata realizzazione del sistema informatico/informativo. -"
        "cura le revisioni periodiche degli estimi catastali e l’attività"
        "di raffittimento della rete geodetica del territorio provinciale",

        'image_url': 'http://dati.trentino.it/images/logo.png',
        'type': 'organization',
        'is_organization': True,
        'state': 'active',
        'tags': [],
    },
}

GROUPS = {
    'gestione-del-territorio': {
        'name': 'gestione-del-territorio',
        'title': 'Gestione del territorio',
        'description': 'Viabilità, idrografia, aree protette, toponomastica, '
        'orografia, uso del suolo, ecc.',
    }
}


def _multistrptime(dt, formats):
    for fmt in formats:
        try:
            return datetime.datetime.strptime(dt, fmt)
        except ValueError:
            pass
    raise ValueError("No format matched")


def _date_to_iso(dt):
    formats = ['%Y-%m-%d', '%Y-%m', '%Y']
    the_date = _multistrptime(dt, formats)
    return the_date.isoformat()


def _clean_tags(tags, tags_map=TAGS_MAP):
    """Clean tags according to the map"""

    def _clean_tag(tag):
        if tag not in tags_map:
            logger.warn('Found an unknown tag: {0!r}'.format(tag))
            return []

        tagvalue = tags_map[tag]
        if (tagvalue is None) or (tagvalue is False):
            return []

        if tagvalue is True:
            return [tag.capitalize()]

        if isinstance(tagvalue, basestring):
            return [tagvalue]

        return tagvalue

    new_tags = set()
    for tag in tags:
        new_tags.update(_clean_tag(tag))
    return sorted(new_tags)


class GeoCatalogoToCkan(ConverterPluginBase):

    def convert(self, storage_in, storage_out):
        logger.info('Converting datasets PAT Geocatalogo -> ckan')

        _num_datasets = len(storage_in.documents['dataset'])
        report_progress(0, _num_datasets)

        all_datasets = storage_in.documents['dataset'].iteritems()
        for i, (dataset_id, dataset) in enumerate(all_datasets):
            logger.info('Converting dataset {0} [{1}/{2}]'
                        .format(dataset_id, i + 1, _num_datasets))

            # We load the XML from the dataset ``raw_xml`` field,
            # then we can extract relevant information.

            search_result_xml = dataset['raw_xml']
            linked_xml = storage_in.blobs['resource_xml'][dataset_id]

            try:

                converted = extract_metadata_from_api_xml(search_result_xml)

                converted['resources'] = get_resources_from_api_xml(
                    search_result_xml, linked_xml)

                extras = extract_metadata_from_linked_xml(linked_xml)
                converted['extras'] = dict(
                    (': '.join(k), v)
                    for k, v in flatten_dict(extras).iteritems())

                # If the website is the one from "catasto", we want to put
                # the dataset in **that** organization
                if extras.get('URL sito') == 'http://www.catasto.provincia.tn.it':  # noqa
                    converted['owner_org'] = ORG_CATASTO

                # Do the actual conversion
                assert int(converted['id']) == dataset_id
                storage_out.documents['dataset'][str(converted['id'])] = converted  # noqa

            except:
                logger.exception('Conversion failed')

            report_progress(i + 1, _num_datasets)

        logger.info('Importing organizations')
        for org_name, org in ORGANIZATIONS.iteritems():
            storage_out.documents['organization'][org_name] = org

        logger.info('Importing groups')
        for group_name, group in GROUPS.iteritems():
            storage_out.documents['group'][group_name] = group


def extract_metadata_from_api_xml(xmldata):
    xmldata = xmldata.decode('latin-1')
    xml = lxml.etree.fromstring(xmldata)
    xph = XPathHelper(xml, nsmap=API_XML_NSMAP)

    def _capfirst(s):
        return s[0].upper() + s[1:]

    _ds_title = xph('dc:title/text()').get_one('')
    _ds_title = _capfirst(_ds_title)
    # _ds_title = normalize_case(_ds_title)

    _ds_name = slugify(_ds_title)
    _ds_license = int(xph('geonet:info/licenseType/text()').get_one())
    _ds_owner = xph('geonet:info/ownername/text()').get_one().title()

    return {
        'id': int(xph('geonet:info/id/text()').get_one()),
        'name': _ds_name,
        'title': _ds_title,
        'notes': xph('dct:abstract/text()').get_one(''),
        'author': _ds_owner,
        'author_email': 'N/A',
        'maintainer': _ds_owner,
        'maintainer_email': 'N/A',
        'url': 'http://www.territorio.provincia.tn.it/',
        'license_id': LICENSES_MAP[_ds_license],
        'owner_org': ORG_SIAT,
        'groups': ['gestione-del-territorio'],  # Fixed
        'extras': {},
        'tags': _clean_tags(xph('dc:subject/text()')),
    }


def get_resources_from_api_xml(xmldata, xmldata2):
    xmldata = xmldata.decode('latin-1')
    xml = lxml.etree.fromstring(xmldata)
    xph = XPathHelper(xml, nsmap=API_XML_NSMAP)

    _description = xph('dct:abstract/text()').get_one('')
    _url_ogd_xml = xph('geonet:info/ogd_xml/text()').get_one()
    _url_ogd_zip = xph('geonet:info/ogd_zip/text()').get_one()
    _url_ogd_rdf = xph('geonet:info/ogd_rdf/text()').get_one()

    links = get_links_from_linked_xml(xmldata2)
    linked_files = {'zip': [], 'xls': []}
    for link in links:
        for ext in ('zip', 'xls'):
            if link.endswith('.' + ext):
                linked_files[ext].append(link)

    if len(linked_files['zip']) > 0:
        if len(linked_files['zip']) > 1:
            logger.warning('Linked XML contains more than one link '
                           'to a zip file! Using first one.')
        if linked_files['zip'][0] != _url_ogd_zip:
            logger.warning('Replacing link to ZIP with one found '
                           'in linked XML')
            _url_ogd_zip = linked_files['zip'][0]

    resources = []

    # ************************************************************
    # NOTE: for some strange reason, format will be converted
    #       to different case depending on the format itself..
    #       So "XML"/"RDF" **must** be upper-case and "shp"
    #       **must** be lower-case, otherwise baby pandas will
    #       get slaughtered.
    # ************************************************************

    if _url_ogd_xml:
        resources.append({
            'name': 'Metadati in formato XML',
            'description': _description,
            'format': 'XML',
            'mimetype': 'application/xml',
            'url': _url_ogd_xml,
        })

    if _url_ogd_rdf:
        resources.append({
            'name': 'Dati in formato RDF',
            'description': _description,
            'format': 'RDF',
            'mimetype': 'application/rdf+xml',
            'url': _url_ogd_rdf,
        })

    if _url_ogd_zip:
        resources.append({
            'name': 'Dati in formato Shapefile',
            'description': _description,
            'format': 'shp',
            'mimetype': 'application/zip',
            'url': _url_ogd_zip,
        })

    if len(linked_files['xls']) > 0:
        logger.info("Found XLS file in linked XML -- assuming "
                    "it is documentation")
        for link in linked_files['xls']:
            resources.append({
                'name': 'Documentazione in formato XLS',
                'description': _description,
                'format': 'xls',
                'mimetype': 'application/vnd.ms-excel',
                'url': link,
            })

    return resources


def get_links_from_linked_xml(xmldata):
    xmldata = xmldata.decode('latin-1')
    xml = lxml.etree.fromstring(xmldata)
    xmlxph = XPathHelper(xml, nsmap=LINKED_XML_NSMAP)

    links_xpath = ('/gmd:MD_Metadata/gmd:distributionInfo/'
                   'gmd:MD_Distribution/gmd:transferOptions/'
                   'gmd:MD_DigitalTransferOptions/gmd:onLine/'
                   'gmd:CI_OnlineResource/gmd:linkage/gmd:URL/text()')
    links = xmlxph(links_xpath)

    return list(links)


def extract_metadata_from_linked_xml(xmldata):
    xmldata = xmldata.decode('latin-1')
    xml = lxml.etree.fromstring(xmldata)
    result = {}

    xmlxph = XPathHelper(xml, nsmap=LINKED_XML_NSMAP)

    metadata = xmlxph('/gmd:MD_Metadata')

    identification_info = metadata(
        'gmd:identificationInfo/gmd:MD_DataIdentification')

    result['Informazioni di Identificazione'] = {
        "Titolo": identification_info(
            "gmd:citation/gmd:CI_Citation/"
            "gmd:title/gco:CharacterString"
            "/text()").get_one(),
        "Data": identification_info(
            "gmd:citation/gmd:CI_Citation/"
            "gmd:date/gmd:CI_Date/gmd:date/gco:Date"
            "/text()").get_one(),
        "Tipo data": identification_info(
            "gmd:citation/gmd:CI_Citation/"
            "gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode"
            "/text()").get_one(),
        "Codice": identification_info(
            "gmd:citation/gmd:CI_Citation/"
            "gmd:identifier/gmd:RS_Identifier/gmd:code/gco:CharacterString"
            "/text()").get_one(),
        "Nome dell'Ente": identification_info(
            "gmd:citation/gmd:CI_Citation/"
            "gmd:citedResponsibleParty/gmd:CI_ResponsibleParty/"
            "gmd:organisationName/gco:CharacterString"
            "/text()").get_one(),
        "Telefono": identification_info(
            "gmd:citation/gmd:CI_Citation/"
            "gmd:citedResponsibleParty/gmd:CI_ResponsibleParty/"
            "gmd:contactInfo/gmd:CI_Contact/"
            "gmd:phone/gmd:CI_Telephone/gmd:voice/gco:CharacterString"
            "/text()").get_one(),
        "E-mail": identification_info(
            "gmd:citation/gmd:CI_Citation/"
            "gmd:citedResponsibleParty/gmd:CI_ResponsibleParty/"
            "gmd:contactInfo/gmd:CI_Contact/"
            "gmd:address/gmd:CI_Address/"
            "gmd:electronicMailAddress/gco:CharacterString"
            "/text()").get_one(),
        "Risorsa Online": identification_info(
            "gmd:citation/gmd:CI_Citation/"
            "gmd:citedResponsibleParty/gmd:CI_ResponsibleParty/"
            "gmd:contactInfo/gmd:CI_Contact/"
            "gmd:onlineResource/gmd:CI_OnlineResource/gmd:linkage/gmd:URL"
            "/text()").get_one(),
        "Ruolo": identification_info(
            "gmd:citation/gmd:CI_Citation/"
            "gmd:citedResponsibleParty/gmd:CI_ResponsibleParty/"
            "gmd:role/gmd:CI_RoleCode"
            "/text()").get_one(),
        "Formato di presentazione": identification_info(
            "gmd:citation/gmd:CI_Citation/"
            "gmd:presentationForm/gmd:CI_PresentationFormCode"
            "/text()").get_one(),
        "Identificatore": identification_info(
            "gmd:citation/gmd:CI_Citation/"
            "gmd:series/gmd:CI_Series/"
            "gmd:issueIdentification/gco:CharacterString"
            "/text()").get_one(),
        "Descrizione": identification_info(
            "gmd:abstract/gco:CharacterString"
            "/text()").get_one(),
        "Punto di Contatto: Nome dell'Ente": identification_info(
            "gmd:pointOfContact/gmd:CI_ResponsibleParty/"
            "gmd:organisationName/gco:CharacterString"
            "/text()").get_one(),
        "Punto di Contatto: Ruolo": identification_info(
            "gmd:pointOfContact/gmd:CI_ResponsibleParty/"
            "gmd:contactInfo/gmd:CI_Contact/"
            "gmd:phone/gmd:CI_Telephone/gmd:voice/gco:CharacterString"
            "/text()").get_one(),
        "Punto di Contatto: Telefono": identification_info(
            "gmd:pointOfContact/gmd:CI_ResponsibleParty/"
            "gmd:contactInfo/gmd:CI_Contact/"
            "gmd:phone/gmd:CI_Telephone/gmd:voice/gco:CharacterString"
            "/text()").get_one(),
        "Punto di Contatto: E-mail": identification_info(
            "gmd:pointOfContact/gmd:CI_ResponsibleParty/"
            "gmd:contactInfo/gmd:CI_Contact/"
            "gmd:address/gmd:CI_Address/"
            "gmd:electronicMailAddress/gco:CharacterString"
            "/text()").get_one(),
        "Punto di Contatto: Risorsa Online": identification_info(
            "gmd:pointOfContact/gmd:CI_ResponsibleParty/"
            "gmd:contactInfo/gmd:CI_Contact/"
            "gmd:onlineResource/gmd:CI_OnlineResource/gmd:linkage/gmd:URL"
            "/text()").get_one(),
        "Parole chiave": identification_info(
            "gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/"
            "gco:CharacterString"
            "/text()").get_one(),
        "Limitazione d'uso": identification_info(
            "gmd:resourceConstraints/gmd:MD_LegalConstraints/"
            "gmd:useLimitation/gco:CharacterString"
            "/text()").get_one(),
        "Vincoli di accesso": identification_info(
            "gmd:resourceConstraints/gmd:MD_LegalConstraints/"
            "gmd:accessConstraints/gmd:MD_RestrictionCode"
            "/text()").get_one(),
        "Vincoli di fruibilita'": identification_info(
            "gmd:resourceConstraints/gmd:MD_LegalConstraints/"
            "gmd:useConstraints"
            "/text()").get_one(),
        "Altri vincoli": identification_info(
            "gmd:resourceConstraints/gmd:MD_LegalConstraints/"
            "gmd:otherConstraints/gco:CharacterString"
            "/text()").get_one(),
        "Tipo di rappresentazione spaziale": identification_info(
            "gmd:spatialRepresentationType/"
            "gmd:MD_SpatialRepresentationTypeCode/"
            "@codeListValue").get_one(),
        "Scala Equivalente: Denominatore": identification_info(
            "gmd:spatialResolution/gmd:MD_Resolution/"
            "gmd:equivalentScale/gmd:MD_RepresentativeFraction/"
            "gmd:denominator/gco:Integer"
            "/text()").get_one(),
        "Lingua": identification_info(
            "gmd:language/gco:CharacterString"
            "/text()").get_one(),
        "Set dei caratteri dei metadati": identification_info(
            "gmd:characterSet/gmd:MD_CharacterSetCode/"
            "@codeListValue").get_one(),
        "Tema": identification_info(
            "gmd:topicCategory/gmd:MD_TopicCategoryCode"
            "/text()").get_one(),
    }
    result['Codifica Caratteri'] = metadata(
        "gmd:characterSet/gmd:MD_CharacterSetCode/"
        "@codeListValue").get_one()

    bounding_box = identification_info(
        'gmd:extent/gmd:EX_Extent/'
        'gmd:geographicElement/gmd:EX_GeographicBoundingBox')

    result['Estensione'] = {
        'Localizzazione Geografica': {
            "Latitudine Nord":
            bounding_box("gmd:northBoundLatitude/text()").get_one(),
            "Longitudine Ovest":
            bounding_box("gmd:westBoundLongitude/text()").get_one(),
            "Longitudine Est":
            bounding_box("gmd:eastBoundLongitude/text()").get_one(),
            "Latitudine Sud":
            bounding_box("gmd:southBoundLatitude/text()").get_one(),
        },
    }

    distribution = metadata('gmd:distributionInfo/gmd:MD_Distribution')
    distribution_format = distribution('gmd:distributionFormat/gmd:MD_Format')
    distributor_contact = distribution(
        'gmd:distributor/gmd:MD_Distributor/'
        'gmd:distributorContact/gmd:CI_ResponsibleParty')

    result['Informazioni sulla Distribuzione'] = {
        "Nome formato": distribution_format(
            "gmd:name/gco:CharacterString"
            "/text()").get_one(),
        "Versione formato": distribution_format(
            "gmd:version/gco:CharacterString"
            "/text()").get_one(),
        "Distributore: Nome dell'Ente": distributor_contact(
            "gmd:organisationName/gco:CharacterString"
            "/text()").get_one(),
        "Distributore: Telefono": distributor_contact(
            "gmd:contactInfo/gmd:CI_Contact/"
            "gmd:phone/gmd:CI_Telephone/gmd:voice/"
            "gco:CharacterString"
            "/text()").get_one(),
        "Distributore: E-mail": distributor_contact(
            "gmd:contactInfo/gmd:CI_Contact/"
            "gmd:address/gmd:CI_Address/gmd:electronicMailAddress/"
            "gco:CharacterString"
            "/text()").get_one(),
        "Distributore: Risorsa Online": distributor_contact(
            "gmd:contactInfo/gmd:CI_Contact/"
            "gmd:onlineResource/gmd:CI_OnlineResource/"
            "gmd:linkage/gmd:URL"
            "/text()").get_one(),
        "Distributore: Ruolo": distributor_contact(
            "gmd:role/gmd:CI_RoleCode/"
            "@codeListValue").get_one(),
        "Opzioni di Trasferimento: Risorsa Online": distributor_contact(
            "gmd:role/gmd:CI_RoleCode/"
            "@codeListValue").get_one(),
    }

    result['Informazioni sul Sistema di Riferimento'] = {
        'Codice': metadata(
            'gmd:referenceSystemInfo/gmd:MD_ReferenceSystem/'
            'gmd:referenceSystemIdentifier/gmd:RS_Identifier/'
            'gmd:code/gco:CharacterString/text()').get_one(),
    }

    quality_info = metadata('gmd:dataQualityInfo/gmd:DQ_DataQuality')
    quantitative_result = quality_info(
        'gmd:report/gmd:DQ_AbsoluteExternalPositionalAccuracy/'
        'gmd:result/gmd:DQ_QuantitativeResult')
    conformance_result = quality_info(
        'gmd:report/gmd:DQ_DomainConsistency/'
        'gmd:result/gmd:DQ_ConformanceResult')

    result[u'Informazioni sulla Qualità dei Dati'] = {
        "Livello": quality_info(
            "gmd:scope/gmd:DQ_Scope/gmd:level/gmd:MD_ScopeCode/"
            "@codeListValue").get_one(),
        "Identificatore": quantitative_result(
            "gmd:valueUnit/gml:BaseUnit/"
            "@gml:id").get_one(),
        "Identificatore: Autorita' competente": quantitative_result(
            "gmd:valueUnit/gml:BaseUnit/gml:identifier/"
            "@codeSpace ").get_one(),
        "Identificatore: Identificatore": quantitative_result(
            "gmd:valueUnit/gml:BaseUnit/gml:identifier/"
            "@codeSpace").get_one(),
        "xlink:href": quantitative_result(
            "gmd:valueUnit/gml:BaseUnit/gml:unitsSystem/"
            "@xlink:href").get_one(),
        "Registrazione": quantitative_result(
            "gmd:value/gco:Record"
            "/text()").get_one(),
        "Titolo": conformance_result(
            "gmd:specification/gmd:CI_Citation/"
            "gmd:title/gco:CharacterString"
            "/text()").get_one(),
        "Data": conformance_result(
            "gmd:specification/gmd:CI_Citation/"
            "gmd:date/gmd:CI_Date/gmd:date/gco:Date"
            "/text()").get_one(),
        "Tipo data": conformance_result(
            "gmd:specification/gmd:CI_Citation/"
            "gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode"
            "/text()").get_one(),
        "Spiegazione": conformance_result(
            "gmd:explanation/gco:CharacterString"
            "/text()").get_one(),
        "Conformità": conformance_result(
            "gmd:pass/gco:Boolean"
            "/text()").get_one(),
        "Processo di produzione": quality_info(
            "gmd:lineage/gmd:LI_Lineage/gmd:statement/gco:CharacterString"
            "/text()").get_one(),

    }

    contact = metadata('gmd:contact/gmd:CI_ResponsibleParty')

    result['Metadato'] = {
        "Identificatore del file dei metadati": metadata(
            "gmd:fileIdentifier/gco:CharacterString"
            "/text()").get_one(),
        "Lingua": metadata(
            "gmd:language/gco:CharacterString"
            "/text()").get_one(),
        "Set dei caratteri dei metadati": metadata(
            "gmd:characterSet/gmd:MD_CharacterSetCode/"
            "@codeListValue").get_one(),
        "Identificatore del file precedente di metadato": metadata(
            "gmd:parentIdentifier/gco:CharacterString"
            "/text()").get_one(),
        "Livello gerarchico": metadata(
            "gmd:hierarchyLevel/gmd:MD_ScopeCode/"
            "@codeListValue").get_one(),
        "Data dei metadati": metadata(
            "gmd:dateStamp/gco:DateTime"
            "/text()").get_one(),
        "Nome dello standard dei metadati": metadata(
            "gmd:metadataStandardName/gco:CharacterString"
            "/text()").get_one(),
        "Versione dello Standard dei metadati": metadata(
            "gmd:metadataStandardVersion"
            "/text()").get_one(),
        "Contatto: Nome dell'Ente": contact(
            "gmd:organisationName/gco:CharacterString"
            "/text()").get_one(),
        "Contatto: Ruolo": contact(
            "gmd:role/gmd:CI_RoleCode/"
            "@codeListValue").get_one(),
        "Contatto: Telefono": contact(
            "gmd:contactInfo/gmd:CI_Contact/"
            "gmd:phone/gmd:CI_Telephone/gmd:voice/"
            "gco:CharacterString"
            "/text()").get_one(),
        "Contatto: E-mail": contact(
            "gmd:contactInfo/gmd:CI_Contact/"
            "gmd:address/gmd:CI_Address/gmd:electronicMailAddress/"
            "gco:CharacterString"
            "/text()").get_one(),
        "Contatto: Risorsa Online": contact(
            "gmd:contactInfo/gmd:CI_Contact/"
            "gmd:onlineResource/gmd:CI_OnlineResource/gmd:linkage/"
            "gmd:URL"
            "/text()").get_one(),
    }

    # Add some fixed values
    result['Titolare'] = 'Provincia Autonoma di Trento'
    result['Copertura Geografica'] = 'Provincia di Trento'
    result['Aggiornamento'] = 'Non programmato'
    result['URL sito'] = identification_info(
        "gmd:citation/gmd:CI_Citation/"
        "gmd:citedResponsibleParty/gmd:CI_ResponsibleParty/"
        "gmd:contactInfo/gmd:CI_Contact/"
        "gmd:onlineResource/gmd:CI_OnlineResource/gmd:linkage/gmd:URL"
        "/text()").get_one()

    # Add some dates
    dataset_date = identification_info(
        "gmd:citation/gmd:CI_Citation/"
        "gmd:date/gmd:CI_Date/gmd:date/gco:Date"
        "/text()").get_one()
    try:
        iso_date = _date_to_iso(dataset_date)
    except ValueError:
        iso_date = ''

    result.update({
        'Data di pubblicazione': iso_date,
        'Data di aggiornamento': iso_date,
        'Data di creazione': iso_date,
    })

    return result
