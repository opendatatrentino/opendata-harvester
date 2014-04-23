import lxml
from owslib.csw import CatalogueServiceWeb

DEFAULT_URL = 'http://www.territorio.provincia.tn.it/geoportlet/srv/eng/csw'


class GeoCatalogoClient(object):
    """
    """

    def __init__(self, url):
        self.url = url

    def iter_datasets(self):
        client = CatalogueServiceWeb(self.catalog_url)
        PAGE_SIZE = 20
        startpos = 0
        while True:
            client.getrecords2(
                resulttype='results_with_summary',
                startposition=startpos,
                maxrecords=PAGE_SIZE)
            nextrecord = client.results['nextrecord']
            matches = client.results['matches']

            # Parse the XML :( and yield results..
            tree = lxml.etree.fromstring(client.response)
            records = tree.xpath(
                '/csw:GetRecordsResponse/csw:SearchResults/csw:SummaryRecord',
                namespaces=tree.nsmap)

            for record in records:
                # Just yield the XML elements..
                yield record

            # Decide whether to continue
            # Apparently, nextrecord is always set to
            # (last id + 1), so we need to check bounds..
            if nextrecord > matches:
                return  # :(
            startpos = nextrecord
