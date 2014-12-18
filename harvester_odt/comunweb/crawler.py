import itertools
import logging
import urlparse

import requests

from harvester.ext.crawler.base import CrawlerPluginBase
from harvester.utils import to_ordinal, report_progress
from . import DEFAULT_CLASSES

logger = logging.getLogger(__name__)


def safe_iter(iterable):
    while True:
        try:
            yield iterable.next()
        except StopIteration:
            return
        except:
            logger.exception()


class ComunWebCrawler(CrawlerPluginBase):
    """
    Crawler for "ComunWeb"-powered websites.

    Will download data from a selection of object "classes",
    download data contained in the page returned by requesting the
    url in the "link" field and store them in the database,
    as a document of "classIdentifier" type, keeping the original id
    from the "objectId" field.

    Objects stored in the storage have the following keys:

    - classIdentifier:
        the class type id, eg. ``"open_data"``
    - dateModified:
        Unix timestamp, as integer
    - datePublished:
        Unix timestamp, as integer
    - fullUrl:
        Link to HTTP page
    - link:
        Link to API/JSON metadata
    - nodeId:
        Numeric id of the node, eg. ``831248``
    - nodeRemoteId:
        16-byte hex string (32 char long) (md5 of something?)
    - objectId:
        Numeric id of the object, eg. ``849404``
    - objectName:
        title of the object, eg. ``"Rendiconto del 2013 (Open Data)"``
    - objectRemoteId:
        16-byte hex string (32 char long) (md5 of something?)
    - full_metadata:
        Metadata object, as returned by the ``link`` url.
        Varies depending on the type of object (see json files in the
        comunweb harvester ``docs`` folder).
    """

    options = []

    def fetch_data(self, storage, limit_classes=DEFAULT_CLASSES):
        logger.info(u"Fetching data from comunweb: {0}".format(self.url))

        classes = list(self._list_object_classes())
        logger.debug(u'Available classes: {0}'.format(u', '.join(
            x['identifier'] for x in classes)))

        if limit_classes:
            classes = [x for x in classes
                       if x['identifier'] in limit_classes]

        logger.info(u'Selected {0} class(es)'.format(len(classes)))
        logger.debug(u'Selected classes: {0}'.format(u', '.join(
            x['identifier'] for x in classes)))

        for clsinfo in classes:
            # Each clsinfo has "identifier", "link", "name"

            logger.info(u"Downloading data from class: {0} ({1}): {2}"
                        .format(clsinfo['identifier'],
                                clsinfo['name'],
                                clsinfo['link']))

            resp = requests.get(clsinfo['link'])
            _progress_total = int(resp.json()['metadata']['count'])
            _progress_next = itertools.count(0).next
            _progress_name = (u'Class: {0}'.format(clsinfo['identifier']),)

            obj_type = clsinfo['identifier']

            # Now iterate all the objects in this class
            objects = self._scan_pages(clsinfo['link'])
            for i, obj in enumerate(safe_iter(objects)):

                report_progress(
                    _progress_name, _progress_next(), _progress_total,
                    'Downloading {0} #{1} [{2}]'.format(
                        obj_type, i, obj['nodeId']))

                # Make sure objects are coherent
                assert obj['classIdentifier'] == obj_type

                node_id = obj['nodeId']
                obj_id = obj['objectId']
                object_name = obj['objectName']
                link = obj['link']

                # ===== NOTE =============================================
                # Objects have two candidate keys: ``objectId`` and
                # ``nodeId``; the latter is used in "link" urls so we
                # now use it as primary object key; the former one can
                # still be used to get metadata by requesting a URL
                # like: /api/opendata/v1/content/object/<objectId> but
                # apparently that contains slightly less information
                # (node info and full url are missing)
                # ========================================================

                logger.debug(
                    u'Storing {seq} object of type "{obj_type}" '
                    u'nodeId={node_id}, objectId={obj_id}, title="{title}"'
                    .format(
                        seq=to_ordinal(i + 1),
                        obj_type=obj_type,
                        node_id=node_id,
                        obj_id=obj_id,
                        title=object_name))

                try:
                    metadata = requests.get(link).json()
                except:
                    logger.exception('Error getting metadata')
                    obj['full_metadata'] = None
                else:
                    obj['full_metadata'] = metadata

                # Store it by nodeId
                storage.documents[obj_type][node_id] = obj

            report_progress(
                _progress_name, _progress_next(), _progress_total, 'All done')

    def _list_object_classes(self):
        """
        Return a list of available "object classes" for the crawled site.

        Each item in the list is a dict like this::

            {
            "identifier": "open_data",
            "link": "http://.../api/opendata/v1/content/class/open_data",
            "name": "Open Data"
            },
        """

        response = requests.get(urlparse.urljoin(
            self.url, '/api/opendata/v1/content/classList'))
        assert response.ok
        return response.json()['classes']

    def _scan_pages(self, start_url):
        """
        Keep downloading pages from a paged API request and yield
        objects found in each page, until the end is reached.

        Each yielded item is a dict like this::

            {
            "classIdentifier": "open_data",
            "dateModified": 1399274108,
            "datePublished": 1399240800,
            "fullUrl": "http://www.comune.trento.it/Comune/Documenti/Bilanci/"
                       "Bilanci-di-rendicontazione/Rendiconti-di-gestione/"
                       "Rendiconto-del-2013/Rendiconto-del-2013-Open-Data",
            "link": "http://www.comune.trento.it"
                    "/api/opendata/v1/content/node/831248",
            "nodeId": 831248,
            "nodeRemoteId": "dc04403fe707a2b5f36efba071bd119e",
            "objectId": 849404,
            "objectName": "Rendiconto del 2013 (Open Data)",
            "objectRemoteId": "260e6a5ebdc2e6319f3353a0d9b2f5bd"
            },

        """

        offset, limit = 0, 50

        while True:
            page_url = '{0}/offset/{1}/limit/{2}'.format(
                start_url.rstrip('/'), offset, limit)
            response = requests.get(page_url)
            nodes = response.json()['nodes']

            if len(nodes) < 1:
                # This was the last page
                return

            for item in nodes:
                yield item
                offset += 1  # for next page
