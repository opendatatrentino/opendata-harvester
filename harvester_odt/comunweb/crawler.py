import logging
import urlparse

import requests

from harvester.ext.crawler.base import CrawlerPluginBase

logger = logging.getLogger(__name__)


class ComunWebCrawler(CrawlerPluginBase):
    """
    Crawler for "ComunWeb"-powered websites.
    """

    options = []

    def fetch_data(self, storage):
        logger.info("Fetching data from comunweb")

        classes = self._list_object_classes()
        for clsinfo in classes:
            logger.info(u"Found class {0} ({1}): {2}"
                        .format(clsinfo['identifier'],
                                clsinfo['name'],
                                clsinfo['link']))

            obj_type = clsinfo['identifier']
            for i, obj in enumerate(self._scan_pages(clsinfo['link'])):
                obj_id = obj['objectId']
                logger.debug(
                    u'Storing "{type}" object #{seq} (id={id!r}): "{title}"'
                    .format(
                        seq=i+1,
                        type=obj_type,
                        id=obj_id,
                        title=obj['objectName']))

                metadata_url = obj['link']
                metadata = requests.get(metadata_url).json()
                obj['full_metadata'] = metadata

                # Store it
                storage.documents[obj_type][obj_id] = obj

    def _list_object_classes(self):
        response = requests.get(urlparse.urljoin(
            self.url, '/api/opendata/v1/content/classList'))
        assert response.ok
        return response.json()['classes']

    def _scan_datasets(self):
        start_url = urlparse.urljoin(
            self.url, '/api/opendata/v1/content/class/open_data')
        return self._scan_pages(start_url)

    def _scan_pages(self, start_url):
        offset, limit = 0, 50

        while True:
            page_url = '{0}/offset/{1}/limit/{2}'.format(
                start_url, offset, limit)
            response = requests.get(page_url)
            nodes = response.json()['nodes']
            if len(nodes) < 1:
                # Was the last page
                return
            for ds in nodes:
                yield ds
                offset += 1
