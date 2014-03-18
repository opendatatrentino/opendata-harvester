"""

Download data from statweb.provincia.tn.it into a folder containing
json files, for use by the import script.

Will also convert original datasets into datasets suitable for
insertion in Ckan.

"""

from __future__ import print_function

import json
import sys
import os

from client_statistica import StatisticaClient


## todo: we need to create organization / group json files, reading
##       from some configuration that will also contain information
##       on how to map categories source -> ckan.


destination = sys.argv[1]
for name in ('dataset', 'group', 'organization'):
    dirname = os.path.join(destination, name)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
del dirname


client = StatisticaClient()
for dataset in client.iter_datasets():
    print("Dataset: {0}".format(dataset['id']))
    destfile = os.path.join(destination, 'dataset', '{0}.json'.format(dataset['id']))
    with open(destfile, 'wb') as f:
        json.dump(dataset, f)
