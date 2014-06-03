"""
Delete old datasets from a catalog.
"""

from __future__ import print_function

import sys
import os
from ckan_api_client.high_level import CkanHighlevelClient

CKAN_URL = os.environ['CKAN_URL']
CKAN_API_KEY = os.environ['CKAN_API_KEY']
ORGANIZATION_NAMES = sys.argv[1:]


def confirm(text='Continue?'):
    while True:
        cont = raw_input('{0} [y/n] '.format(text)).lower().strip()
        if cont == 'y':
            return True
        if cont == 'n':
            return False
        print("Invalid answer")


def abort():
    print("Aborted")
    sys.exit(1)


client = CkanHighlevelClient(CKAN_URL, api_key=CKAN_API_KEY)

# First, get the organization id
# Then, create list of datasets to be delted
# Then, delete them.

organizations = set()

for org_name in ORGANIZATION_NAMES:
    organization = client.get_organization_by_name(org_name)
    print("Found organization")
    print("Id: {0}".format(organization.id))
    print("Name: {0}".format(organization.name))
    print("Title: {0}".format(organization.title))
    print("")
    organizations.add(organization.id)

confirm() or abort()

datasets_to_delete = set()

print("\nLooking for organization datasets")
print("Starting...")  # Placeholder
for i, dataset in enumerate(client.iter_datasets()):
    print("\033[1A\033[KProcessed: {0} Found: {1}"
          .format(i, len(datasets_to_delete)))
    if dataset.owner_org in organizations:
        datasets_to_delete.add(dataset.id)

print("\nWill delete {0} datasets.".format(len(datasets_to_delete)))
confirm() or abort()

print("\nDeleting datasets")
print("Starting...")  # Placeholder
for i, dataset_id in enumerate(datasets_to_delete):
    client.wipe_dataset(dataset_id)
    print("\033[1A\033[KDeleted: {0}/{1}"
          .format(i + 1, len(datasets_to_delete)))

print("\n\nAll done")
