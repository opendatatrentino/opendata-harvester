#!/usr/bin/env python

"""
Script to download data from Statistica/StatisticaSubPro
into a sqlite database.

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

import json
import logging
import os
import re
import sqlite3
import sys

from client_statistica import StatisticaClient, CATEGORIES

logger = logging.getLogger(__name__)


class BaseDBWrapper(object):
    def __init__(self, filename):
        self.filename = filename

    @property
    def connection(self):
        if getattr(self, '_connection', None) is None:
            self._connection = sqlite3.connect(self.filename)
        return self._connection

    def cursor(self, *a, **kw):
        return self.connection.cursor(*a, **kw)

    def execute(self, *a, **kw):
        return self.cursor().execute(*a, **kw)

    def commit(self):
        self.connection.commit()

    def create_tables(self):
        pass

    def _check_table_name(self, name):
        if not re.match(r'^[A-Za-z0-9_]+$', name):
            raise ValueError("Invalid table name")


class RawDataDB(BaseDBWrapper):
    def create_tables(self):
        c = self.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS "dataset_statistica"
        ( id INT PRIMARY KEY, metadata_json TEXT );
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS "dataset_statistica_subpro"
        ( id INT PRIMARY KEY, metadata_json TEXT );
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS "group"
        ( id VARCHAR(128) PRIMARY KEY, metadata_json TEXT );
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS "organization"
        ( id VARCHAR(128) PRIMARY KEY, metadata_json TEXT );
        """)
        self.commit()

    def upsert(self, table, record):
        self._check_table_name(table)
        if table in ('group', 'organization'):
            key = record['name']
        else:
            key = record['id']
        self.execute('DELETE FROM "{0}" WHERE id=?;'.format(table),
                     (key,))
        query = ('INSERT INTO "{0}" (id, metadata_json) VALUES (?, ?);'
                 .format(table))
        self.execute(query, (key, json.dumps(record)))
        self.commit()


##-----------------------------------------------------------------------------
## Actual operations
##-----------------------------------------------------------------------------

class Downloader(object):
    def __init__(self, filename):
        self.db = RawDataDB(filename)
        self.db.create_tables()

    def download_datasets_statistica(self):
        client = StatisticaClient()
        for dataset in client.iter_raw_datasets():
            logger.info("Dataset: {0}".format(dataset['id']))
            self.db.upsert('dataset_statistica', dataset)

    def download_datasets_statistica_subpro(self):
        pass

    def download_groups(self):
        for key, val in CATEGORIES.iteritems():
            self.db.upsert('group', val)

    def download_organizations(self):
        self.db.upsert('organization', {
            "name": "pat-s-statistica",
            "title": "PAT S. Statistica",
            "description":
            "Censimenti, analisi, indagine statistiche, indicatori, ...",
            "image_url": "http://dati.trentino.it/images/logo.png",
            "type": "organization",
            "is_organization": True,
            "state": "active",
            "tags": [],
        })


if __name__ == '__main__':
    ## Make sure we have an handler for the root logger
    _logger = logging.getLogger()
    _logger.addHandler(logging.StreamHandler(sys.stderr))
    _logger.setLevel(logging.DEBUG)

    destfile = os.path.join(
        os.path.dirname(__file__),
        '.statistica-140320-rawdata.sqlite')

    ## Do stuff!
    downloader = Downloader(destfile)
    downloader.download_datasets_statistica()
    downloader.download_datasets_statistica_subpro()
    downloader.download_groups()
    downloader.download_organizations()
