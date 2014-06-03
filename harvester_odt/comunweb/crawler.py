import logging
import urlparse

import requests

from harvester.ext.crawler.base import CrawlerPluginBase

logger = logging.getLogger(__name__)

# http://www.comune.trento.it/api/opendata/v1/content/class/open_data/offset/10/limit/5


class ComunWebCrawler(CrawlerPluginBase):
    """
    Crawler for "ComunWeb"-powered websites.
    """

    options = []

    def fetch_data(self, storage):
        logger.info("Fetching data from comunweb")

        for i, dataset in enumerate(self._scan_datasets()):
            logger.info(
                u'Storing dataset {seq}: id={id!r} "{title}"'
                .format(
                    seq=i+1,
                    id=dataset['objectId'],
                    title=dataset['objectName']))

            metadata_url = dataset['link']
            metadata = requests.get(metadata_url).json()
            dataset['full_metadata'] = metadata

            # Store it
            storage.documents['dataset'][dataset['objectId']] = dataset

    def _scan_datasets(self):
        offset, limit = 0, 50
        start_url = urlparse.urljoin(
            self.url, '/api/opendata/v1/content/class/open_data')

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
