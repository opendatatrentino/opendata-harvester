#!/usr/bin/env python

"""

Download data from statweb.provincia.tn.it into a folder containing
json files, for use by the import script.

Will also convert original datasets into datasets suitable for
insertion in Ckan.

"""

from __future__ import print_function

import json
import logging
import sys
import os

from client_statistica import StatisticaClient, CATEGORIES


## todo: we need to create organization / group json files, reading
##       from some configuration that will also contain information
##       on how to map categories source -> ckan.

_logger = logging.getLogger()  # root logger
_logger.addHandler(logging.StreamHandler(sys.stderr))
_logger.setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)


destination = sys.argv[1]
for name in ('dataset', 'group', 'organization'):
    dirname = os.path.join(destination, name)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
del dirname


client = StatisticaClient()
for dataset in client.iter_datasets():
    logger.info("Dataset: {0}".format(dataset['id']))
    destfile = os.path.join(destination, 'dataset',
                            '{0}.json'.format(dataset['id']))
    with open(destfile, 'wb') as f:
        json.dump(dataset, f)

## Write organization to file
destfile = os.path.join(destination, 'organization', 'pat-s-statistica.json')
with open(destfile, 'wb') as f:
    json.dump({
        "name": "pat-s-statistica",
        "description":
        "Censimenti, analisi, indagine statistiche, indicatori, ...",
        "display_name": "PAT S. Statistica",
        "image_url": "http://dati.trentino.it/images/logo.png",
        "is_organization": True,
        "state": "active",
        "tags": [],
        "title": "PAT S. Statistica",
        "type": "organization"
    }, f)

## Write groups to file
for key, val in CATEGORIES.iteritems():
    destfile = os.path.join(destination, 'group', '{0}.json'.format(key))
    with open(destfile, 'wb') as f:
        json.dump(val, f)
