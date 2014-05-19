import logging

import lxml
import requests

from harvester.ext.crawler.base import CrawlerPluginBase
from .client import GeoCatalogoClient

logger = logging.getLogger(__name__)


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

        # We iterate all the datasets in the catalog and store
        # them as raw xml, by id.
        # We assume we can use the <dc:identifier> as id, for the moment.
        # The xml data is stored in the "raw_xml" field.

        logger.debug("Iterating datasets from geocatalogo")
        for i, dataset in enumerate(client.iter_datasets()):
            logger.debug("Processing dataset {0}".format(i))

            xp = lambda x: dataset.xpath(x, namespaces=dataset.nsmap)

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
                'raw_xml': dataset_xml,

                # todo: we should figure out some way to convert xml -> json
                # in order to easily perform further inspection on objects..
            }

            obj['urls'] = {}
            for fmt in ('xml', 'zip', 'rdf'):
                found = xp('geonet:info/ogd_{0}/text()'.format(fmt))
                if len(found) < 1:
                    continue
                url = found[0]
                if url:
                    obj['urls'][fmt] = url

            # Actually store the thing in the storage
            logger.info(
                u'Storing dataset: {id!r} ({title!r})'
                .format(id=dataset_id,
                        title=obj['title']))

            # Store information as a document and XML as a blob
            storage.documents['dataset'][dataset_id] = obj
            storage.blobs['dataset'][dataset_id] = dataset_xml

            # Now download resources and store as blobs
            for fmt, url in obj['urls'].iteritems():
                logger.info(u"Downloading {0} resource from {1}"
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
