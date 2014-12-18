import logging

import lxml
import requests

from harvester.ext.crawler.base import CrawlerPluginBase
from harvester.utils import report_progress

from .client import GeoCatalogoClient
from .constants import API_XML_NSMAP

logger = logging.getLogger(__name__)


DOWNLOAD_FORMATS = ('rdf', 'xml')


class Geocatalogo(CrawlerPluginBase):
    """
    Crawler for http://www.territorio.provincia.tn.it/
    """

    options = [
        ('with_resources', 'bool', True,
         'Whether to download resources (data) too.'),
    ]

    def fetch_data(self, storage):
        logger.info("Fetching data from geocatalogo")

        client = GeoCatalogoClient(self.url)

        progress_total = client.count_datasets()

        # We iterate all the datasets in the catalog and store
        # them as raw xml, by id.
        # We assume we can use the <dc:identifier> as id, for the moment.
        # The xml data is stored in the "raw_xml" field.

        # Note: apparently, in many cases the reported total
        # mismatches the number of datasets when iterating..
        # This is a known bug of the external service, and
        # we can't do much to fix it..

        logger.debug("Iterating {0} datasets from geocatalogo"
                     .format(progress_total))
        report_progress(('datasets',), 0, progress_total)

        for i, dataset in enumerate(client.iter_datasets()):
            logger.debug("Processing dataset {0}".format(i))

            xp = lambda x: dataset.xpath(x, namespaces=API_XML_NSMAP)

            dataset_xml = lxml.etree.tostring(dataset)
            dataset_id = int(xp('geonet:info/id/text()')[0])

            obj = {
                # ...keys
                'keys': {
                    'identifier': xp('dc:identifier/text()')[0],
                    'geonet_info_id': xp('geonet:info/id/text()')[0],
                    'geonet_info_source': xp('geonet:info/source/text()')[0],
                    'geonet_info_uuid': xp('geonet:info/uuid/text()')[0],
                    'geonet_info_guid': xp('geonet:info/guid/text()')[0],
                    'geonet_info_parentid':
                    xp('geonet:info/parentid/text()')[0],
                    'geonet_info_uuid_escaped':
                    xp('geonet:info/uuidEscaped/text()')[0],
                },

                # ...other interesting stuff
                'title': xp('dc:title/text()')[0],
                'subject': xp('dc:subject/text()'),

                # ...the raw XML
                # todo: there should be no need to store it here..
                'raw_xml': dataset_xml,

                # URLs to resources
                'urls': {},
            }

            for fmt in ('xml', 'zip', 'rdf'):
                found = xp('geonet:info/ogd_{0}/text()'.format(fmt))
                if len(found) < 1:
                    continue
                url = found[0]
                if url:
                    obj['urls'][fmt] = url

            logger.info(
                u'Storing dataset: {id!r} ({title!r})'
                .format(id=dataset_id, title=obj['title']))

            # Store the "converted" json document
            storage.documents['dataset'][dataset_id] = obj

            # Store the original dataset, as XML
            storage.blobs['dataset'][dataset_id] = dataset_xml

            # Now download resources and store as blobs
            for fmt, url in obj['urls'].iteritems():
                if fmt not in DOWNLOAD_FORMATS:
                    logger.info(
                        u"Skipping download of resource of type {0} (from {1})"
                        .format(fmt, url))
                    continue

                logger.info(u"Downloading resource of type {0} (from {1})"
                            .format(fmt, url))
                response = requests.get(url)
                if response.ok:
                    data = response.content
                    logger.debug(u'Got {0} response, type {1!r}, size {2}'
                                 .format(response.status_code,
                                         response.headers.get(
                                             'content-type', 'unknown'),
                                         len(data)))
                    # blob_id = '{0}_{1}'.format(dataset_id, fmt)
                    # storage.blobs['resource'][blob_id] = data
                    bucket_id = 'resource_{0}'.format(fmt)
                    storage.blobs[bucket_id][dataset_id] = data

                else:
                    logger.error(u'Failed downloading {0} (code: {1})'
                                 .format(url, response.status_code))

            report_progress(('datasets',), i + 1, progress_total)
