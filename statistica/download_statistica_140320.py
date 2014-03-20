#!ipython -i

"""
Script to download data from Statistica/StatisticaSubPro
into sqlite databases for further cleanup / retrieval.

Raw data will be stored as:

Will create the following tables:

- dataset_statistica
- dataset_statistica_subpro
- group
- organization

containing raw data from the source.

Provides facilities for creating another SQLite database
with the following tables:

- dataset
- group
- organization

containing data ready for insertion into Ckan
"""

from __future__ import print_function

import logging
import os
import sys

from client_statistica import (StatisticaClient, StatisticaSubproClient,
                               CATEGORIES, ORGANIZATIONS,
                               dataset_statistica_to_ckan,
                               dataset_statistica_subpro_to_ckan)
from sqlite_kvstore import SQLiteKeyValueStore

logger = logging.getLogger(__name__)


class RawDataDB(SQLiteKeyValueStore):
    def create_tables(self):
        self._create_table('dataset_statistica', numeric_key=True)
        self._create_table('dataset_statistica_subpro', numeric_key=True)


class CleanDataDB(SQLiteKeyValueStore):
    def create_tables(self):
        self._create_table('dataset_statistica', numeric_key=True)
        self._create_table('dataset_statistica_subpro', numeric_key=True)
        self._create_table('group', numeric_key=False)
        self._create_table('organization', numeric_key=False)


##-----------------------------------------------------------------------------
## Actual operations
##-----------------------------------------------------------------------------

class Downloader(object):
    def __init__(self, filename):
        self.db = RawDataDB(filename)
        self.db.create_tables()

    def download_datasets_statistica(self):
        client = StatisticaClient()
        for dataset in client.iter_datasets(clean=False):
            logger.info('Dataset: {0}'.format(dataset['id']))
            self.db.set('dataset_statistica', dataset['id'], dataset)

    def download_datasets_statistica_subpro(self):
        client = StatisticaSubproClient()
        for dataset in client.iter_datasets(clean=False):
            logger.info('Dataset: {0}'.format(dataset['id']))
            self.db.set('dataset_statistica_subpro', dataset['id'], dataset)


def convert_data(infile, outfile):
    indb = RawDataDB(infile)
    outdb = CleanDataDB(outfile)
    outdb.create_tables()

    ##------------------------------------------------------------
    ## Convert datasets for statistica

    for dataset in indb.get_all('dataset_statistica'):
        logger.info("Converting dataset statistica {0}".format(dataset['id']))
        clean_dataset = dataset_statistica_to_ckan(dataset)
        outdb.set('dataset_statistica', clean_dataset['id'], clean_dataset)

    ##------------------------------------------------------------
    ## Convert datasets for statistica_subpro

    for dataset in indb.get_all('dataset_statistica_subpro'):
        logger.info("Converting dataset statistica sub-pro {0}"
                    .format(dataset['id']))
        clean_dataset = dataset_statistica_subpro_to_ckan(dataset)
        outdb.set('dataset_statistica_subpro', clean_dataset['id'],
                  clean_dataset)

    ##------------------------------------------------------------
    ## Groups are hard-coded

    logger.info("Adding groups")
    for group in CATEGORIES.itervalues():
        outdb.set('group', group['name'], group)

    ##------------------------------------------------------------
    ## Organizations are hard-coded

    logger.info("Adding organization")
    for org in ORGANIZATIONS.itervalues():
        outdb.set('organization', org['name'], org)


class App(object):
    def __init__(self, dest_base=None):
        ## Make sure we have an handler for the root logger
        _logger = logging.getLogger()
        _logger.addHandler(logging.StreamHandler(sys.stderr))
        _logger.setLevel(logging.DEBUG)

        if dest_base is None:
            dest_base = os.getcwd()

        self.raw_data_file = os.path.join(
            dest_base, '.statistica-140320-rawdata.sqlite')
        self.clean_data_file = os.path.join(
            dest_base, '.statistica-140320-clean.sqlite')

    @property
    def downloader(self):
        if not hasattr(self, '_downloader'):
            self._downloader = Downloader(self.raw_data_file)
        return self._downloader

    def download_raw_data(self):
        self.download_datasets_statistica()
        self.download_datasets_statistica_subpro()

    def download_datasets_statistica(self):
        return self.downloader.download_datasets_statistica()

    def download_datasets_statistica_subpro(self):
        return self.downloader.download_datasets_statistica_subpro()

    def cleanup_data(self):
        if os.path.exists(self.clean_data_file):
            os.unlink(self.clean_data_file)
        convert_data(self.raw_data_file, self.clean_data_file)


def is_interactive_mode():
    return sys.flags.interactive


INT_MODE_WELCOME = """
**********************************************************************

      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        Welcome to downloader script for statistica datasets
      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    First, you need to instantiate the application:

        app = App()  # save stuff in current directory
        app = App('/tmp')  # save stuff in /tmp/

    Then, you can download some data:

        app.download_raw_data()

    Or download individual sets with:

        app.download_datasets_statistica()
        app.download_datasets_statistica_subpro()

    To cleanup the data, just use:

        app.cleanup_data()

**********************************************************************
"""


if __name__ == '__main__':
    print(INT_MODE_WELCOME)

    # else:
    #     ## When run in non-interactive mode, store
    #     ## files in the same path of this script.
    #     app = App(os.path.dirname(__file__))
    #     app.download_raw_data()
    #     app.cleanup_data()
