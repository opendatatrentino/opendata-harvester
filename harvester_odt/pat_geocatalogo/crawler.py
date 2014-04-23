import logging

import lxml

from harvester.ext.crawlers.base import HarvesterPluginBase
from .client import GeoCatalogoClient

logger = logging.getLogger(__name__)


class Geocatalogo(HarvesterPluginBase):
    """
    Crawler for http://www.territorio.provincia.tn.it/

    Configuration options:

    - ``bruteforce_find`` (bool, default=False):
      if set to True, will use brute force to
      find datasets, instead of just listing them.
    """

    def fetch_data(self, storage):
        logger.info("Fetching data from geocatalogo")

        client = GeoCatalogoClient(self.url)

        # We iterate all the datasets in the catalog and store
        # them as raw xml, by id.
        # We assume we can use the <dc:identifier> as id, for the moment.
        # The xml data is stored in the "raw_xml" field.

        for i, dataset in enumerate(client.iter_datasets()):
            xp = lambda x: dataset.xpath(x, namespaces=dataset.nsmap)

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
                'raw_xml': lxml.etree.tostring(dataset),

                # todo: we should figure out some way to convert xml -> json
                # in order to easily perform further inspection on objects..
            }

            # Actually store the thing in the storage
            logger.info(
                u'Storing dataset: {id!r} ({title!r})'
                .format(id=dataset_id,
                        title=obj['title']))
            storage.set_object('dataset', dataset_id, obj)
