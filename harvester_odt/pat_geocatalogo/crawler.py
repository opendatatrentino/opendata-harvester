import logging

import lxml
from owslib.csw import CatalogueServiceWeb

from harvester.ext.crawlers.base import HarvesterPluginBase
from .client import GeoCatalogoClient


class Geocatalogo(HarvesterPluginBase):
    """
    Crawler for http://www.territorio.provincia.tn.it/

    Configuration options:

    - ``bruteforce_find`` (bool, default=False):
      if set to True, will use brute force to
      find datasets, instead of just listing them.
    """

    logger = logging.getLogger(__name__)

    def fetch_data(self, storage):
        self.logger.info("Fetching data from geocatalogo")

        client = GeoCatalogoClient(self.url)

        # storage.set('dataset', 'id', obj)
