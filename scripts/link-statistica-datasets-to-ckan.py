#!/usr/bin/env python

"""
We need to link "statistica" datasets to the ones already in Ckan.
The only key we can use are resource URLs; it might be nice to be able to
fuzzy-match titles too, in order to search for possible mistakes..

The goal of this script is to go and set ``_harvest_source`` to
``statistica:<id>`` and ``statistica_subpro:<id>`` on the catalog before
updating them with the harvester, in order to remove duplicates.
"""

from __future__ import print_function

import sys
from collections import defaultdict
from harvester.ext.storage.mongodb import MongodbStorage
from ckan_api_client.high_level import CkanHighlevelClient
from ckan_api_client.syncing import HARVEST_SOURCE_ID_FIELD

TARGET_CKAN_URL = 'http://127.0.0.1:5000'
TARGET_CKAN_APIKEY = 'e812da30-dc64-4dac-a3ff-f3fb0ed536ec'


# DOWNLOAD DATA FROM CKAN
# -----------------------

# HARVESTER="harvester -vvv --debug"
# DATABASE=mongodb+mongodb://database.local/harvester_data

# "${HARVESTER}" crawl --crawler ckan+http://dati.trentino.it \
#     --storage "${DATABASE}"/ckan_dti

# "${HARVESTER}" crawl --crawler pat_statistica \
#     --storage "${DATABASE}"/statistica


# DOWNLOAD DATA FROM STATISTICA
# -----------------------------

# "${HARVESTER}" crawl \
#     --crawler pat_statistica \
#     --storage "${DATABASE}"/statistica

# "${HARVESTER}" convert \
#     --converter pat_statistica_to_ckan \
#     --input "${DATABASE}"/statistica \
#     --output "${DATABASE}"/statistica_clean


# DOWNLOAD DATA FROM STATISTICA-SUBPRO
# ------------------------------------

# "${HARVESTER}" crawl \
#     --crawler pat_statistica_subpro \
#     --storage "${DATABASE}"/statistica_subpro

# "${HARVESTER}" convert \
#     --converter pat_statistica_subpro_to_ckan \
#     --input "${DATABASE}"/statistica_subpro \
#     --output "${DATABASE}"/statistica_subpro_clean


# Initialize storages
# -------------------

db_url = 'mongodb://database.local/harvester_data'

ckan_collection = 'ckan_dti'
source_collections = [
    ('statistica', 'statistica_clean'),
    ('statistica_subpro', 'statistica_subpro_clean'),
]

ckan_storage = MongodbStorage('/'.join((db_url, ckan_collection)))
source_storages = dict(
    (name, MongodbStorage('/'.join((db_url, coll))))
    for name, coll in source_collections
)


# Get id of the "statistica" organization
# ---------------------------------------

def get_organization_id(org_name):
    orgs = list(ckan_storage.iter_objects('organization'))
    ids = [(o['name'], o['id']) for o in orgs if o['name'] == org_name]
    assert len(ids) == 1
    assert ids[0][0] == org_name
    return ids[0][1]

organization_id = get_organization_id('pat-s-statistica')


# ------------------------------------------------------------
#                          STRATEGY
# ------------------------------------------------------------

# - Get a list of datasets to be imported into ckan
# - Figure out which ones are already there -> associate
# - Figure out which datasets needs deletion -> delete
# - Figure out which datasets should be added (but do nothing)

# ------------------------------------------------------------


# Create a list to associate ckan names to ids
# plus a list of names of datasets that are in the organization
ckan_names_pool = {}
ckan_organization_datasets = set()

ckan_datasets_by_name = {}
ckan_datasets_by_id = {}

for dataset in ckan_storage.iter_objects('dataset'):
    ckan_datasets_by_name[dataset['name']] = dataset
    ckan_datasets_by_id[dataset['id']] = dataset
    ckan_names_pool[dataset['name']] = dataset['id']
    if dataset['owner_org'] == organization_id:
        ckan_organization_datasets.add(dataset['name'])

num_ckan_datasets = len(ckan_names_pool)
num_ckan_organization_datasets = len(ckan_organization_datasets)

# Figure out which datasets should be associated to
# {ckan_id: (source_name, source_id)}
datasets_to_associate = {}
conflicting_datasets = set()
num_datasets_to_create = 0
num_datasets_in_sources = 0
num_datasets_by_source = defaultdict(lambda: 0)

source_datasets_by_source = defaultdict(dict)

for source_name, storage in source_storages.iteritems():
    for dataset in storage.iter_objects('dataset'):
        num_datasets_in_sources += 1
        num_datasets_by_source[source_name] += 1

        name = dataset['name']
        source_datasets_by_source[source_name][dataset['id']] = dataset

        if name in ckan_organization_datasets:
            id = ckan_names_pool[name]
            ckan_organization_datasets.remove(name)
            datasets_to_associate[id] = (source_name, dataset['id'])
        else:
            if name in ckan_names_pool:
                # The name exists but is not in the interesting org
                conflicting_datasets.add(name)
            num_datasets_to_create += 1


datasets_to_delete = set()
for ds_name in sorted(ckan_organization_datasets):
    datasets_to_delete.add(ckan_datasets_by_name[ds_name]['id'])


# Datasets left in the pool should now be deleted

# Print report
# ------------------------------------------------------------

try:
    import py.io
    TERM_WIDTH = py.io.get_terminal_width()
except:
    TERM_WIDTH = 80


def title(text, color='37', char='-'):
    text = ' {0} '.format(text)
    print('\033[{0}m{1}\033[0m'
          .format(color, text.center(TERM_WIDTH, char)))

title('Activity report', '1;37', '=')

title('Ckan status', '1;32')
print('    total datasets: {0}'.format(num_ckan_datasets))
print('    org. datasets: {0}'.format(num_ckan_organization_datasets))
print()

title('Source status', '1;32')
for name, count in num_datasets_by_source.iteritems():
    print('    {0}: {1}'.format(name, count))
print('    total: {0}'.format(num_datasets_in_sources))
print()

title('Planned tasks', '1;32')
print("Operations that should be executed on Ckan".center(TERM_WIDTH))
print('    Datasets to associate: {0}'.format(len(datasets_to_associate)))
print('    Conflicting datasets: {0}'.format(len(conflicting_datasets)))
print('    Datasets to delete: {0}'.format(len(ckan_organization_datasets)))
print('    New datasets: {0}'.format(num_datasets_to_create))
print()

title('Datasets to be associated', '1;31')
# print("Datasets that are not in the organization or which name is used "
#       "in multiple sources.\n".center(TERM_WIDTH))
for ds, (s_name, s_id) in sorted(datasets_to_associate.iteritems()):
    print('http://dati.trentino.it/dataset/{0} -> {1}:{2}'
          .format(ds, s_name, s_id))
    _ckan_title = ckan_datasets_by_id[ds]['title']
    _src_title = source_datasets_by_source[s_name][s_id]['title']
    if _ckan_title != _src_title:
        print('    \033[31m{0!r}\033[0m -> \033[32m{1!r}\033[0m'
              .format(_ckan_title, _src_title))
    else:
        print('    \033[32m{0!r}\033[0m'.format(_ckan_title))
print()

title('Conflicting datasets', '1;31')
print("Datasets that are not in the organization or which name is used "
      "in multiple sources.\n".center(TERM_WIDTH))
for ds in sorted(conflicting_datasets):
    print('http://dati.trentino.it/dataset/{0}'.format(ds))
print()

title('Datasets to delete', '1;31')
print("Datasets that are in the ckan organization but don't exist anymore "
      "on the sources.\n".center(TERM_WIDTH))
for ds in sorted(ckan_organization_datasets):
    print('http://dati.trentino.it/dataset/{0}'.format(ds))
    print('    \033[36m{0}\033[0m'
          .format(ckan_datasets_by_name[ds]['title'].encode('utf-8')))
print()

# ---------------------------------------------------------------------------
# todo: output data in some format that can be processed by some other
#       script in order to do the changes (or just prompt for confirmation
#       and apply required changes).
# ---------------------------------------------------------------------------

title('Operations to be executed', '1;31', '=')

print("Update:")
for ds, (s_name, s_id) in sorted(datasets_to_associate.iteritems()):
    print('    {0} -> {1}:{2}'.format(ds, s_name, s_id))
print()

print("Delete:")
for ds in datasets_to_delete:
    print('    {0}'.format(ds))
print()

resp = raw_input('Confirm? [y/N] ')
if resp.lower() != 'y':
    print("Aborted.")
    sys.exit(1)

client = CkanHighlevelClient(TARGET_CKAN_URL, api_key=TARGET_CKAN_APIKEY)

for ds, (s_name, s_id) in sorted(datasets_to_associate.iteritems()):
    print('Update {0}: source={1}:{2}'.format(ds, s_name, s_id))
    dataset = client.get_dataset(ds)
    dataset.extras[HARVEST_SOURCE_ID_FIELD] = '{0}:{1}'.format(s_name, s_id)
    client.update_dataset(dataset)

for ds in datasets_to_delete:
    print('Delete: {0}'.format(ds))
    client.delete_dataset(ds)

print()
print("Ok, now you can run the harvester to import stuff.")
print("Good luck!")
