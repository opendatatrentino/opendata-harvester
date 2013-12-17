"""
List all categories from datasets in database
"""

from __future__ import print_function

from collections import defaultdict
import hashlib
import json
import os
import sqlite3

import requests


HERE = os.path.abspath(os.path.dirname(__file__))
DATABASE = os.path.join(HERE, 'statistica.db')


# We want to extract information from datasets, such as:
# - all metadata fields
# - all categories
# - missing / duplicate data

conn = sqlite3.connect(DATABASE)
c = conn.cursor()

ind_backrefs = defaultdict(list)
num_backrefs = defaultdict(list)
den_backrefs = defaultdict(list)
metadata_fields = set()
metadata_values = defaultdict(set)

c.execute("SELECT id, metadata, indicatore_id, numeratore_id,"
          "denominatore_id FROM datasets")
for row in c.fetchall():
    (dataset_id, raw_metadata, indicatore_id, numeratore_id,
     denominatore_id) = row
    try:
        metadata = json.loads(raw_metadata)
    except:
        metadata = {}
    ind_backrefs[indicatore_id].append(dataset_id)
    num_backrefs[numeratore_id].append(dataset_id)
    den_backrefs[denominatore_id].append(dataset_id)

    for key, value in metadata.iteritems():
        metadata_fields.add(key)
        metadata_values[key].add(value)

## Print information
MAX_ITEMS = 50
print("----- Metadata fields -----")
for field in sorted(metadata_fields):
    print("    " + field)
    numvals = len(metadata_values[field])
    if numvals > MAX_ITEMS:
        for value in sorted(metadata_values[field])[:10]:
            print("        " + value)
        print("        [{0} values omitted]".format(numvals - 10))
    else:
        for value in sorted(metadata_values[field]):
            print("        " + value)
