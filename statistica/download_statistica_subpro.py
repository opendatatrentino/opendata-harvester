"""
Download indicatori strutturali subprovinciali
"""


from __future__ import print_function

import copy
import hashlib
import json
import os
import sqlite3
import urllib

import requests


HERE = os.path.abspath(os.path.dirname(__file__))
DATABASE = os.path.join(HERE, 'statistica_subpro.db')

BASE_URL = 'http://www.statweb.provincia.tn.it/'\
           'indicatoristrutturalisubpro/exp.aspx'

INDEX_URL = BASE_URL + "?list=i&fmt=json"

# # Data/Metadata of "numeratore" / "denominatore"
# TAB_METADATA_URL = BASE_URL + "?ntab={name}&info=md&fmt=json"
# TAB_DATA_URL = BASE_URL + "?ntab={name}&info=d&fmt=json"

# # Data/metadata of "indicatori"
# IND_METADATA_URL = BASE_URL + "?idind={id}&info=md&fmt=json"
# IND_DATA_URL = BASE_URL + "?idind={id}&info=d&fmt=json"


def get_tab_metadata_url(name):
    return '?'.join((BASE_URL, urllib.urlencode({
        'ntab': name.encode('utf-8'),
        'info': 'md',
        'fmt': 'json',
    })))


def get_tab_data_url(name):
    return '?'.join((BASE_URL, urllib.urlencode({
        'ntab': name.encode('utf-8'),
        'info': 'd',
        'fmt': 'json',
    })))


def get_ind_metadata_url(id):
    return '?'.join((BASE_URL, urllib.urlencode({
        'idind': str(id),
        'info': 'md',
        'fmt': 'json',
    })))


def get_ind_data_url(id):
    return '?'.join((BASE_URL, urllib.urlencode({
        'idind': str(id),
        'info': 'md',
        'fmt': 'json',
    })))


def create_tables(conn):
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS datasets
    ( id VARCHAR(128) PRIMARY KEY,
      title TEXT,
      metadata TEXT,
      metadata_hash TEXT,
      data TEXT,
      data_hash TEXT );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS dataset_relationships
    (indicatore, numeratore, denominatore,
     PRIMARY KEY (indicatore, numeratore, denominatore))
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS errors
    (id INT PRIMARY KEY, message TEXT);
    """)
    conn.commit()


def log_error(conn, message):
    c = conn.cursor()
    c.execute("INSERT INTO errors (message) "
              "VALUES (?)", (message,))
    conn.commit()


def save_dataset(conn, dataset):
    """Save a dataset in the database"""

    assert 'id' in dataset
    dataset = copy.deepcopy(dataset)
    c = conn.cursor()
    fields = "id metadata metadata_hash data data_hash".split()
    row = tuple(dataset[k] for k in fields)
    try:
        c.execute("""
                  INSERT INTO datasets ({fields})
                  VALUES (?, ?, ?, ?, ?)
                  """.format(fields=', '.join(fields)), row)
    except sqlite3.IntegrityError:
        pass  # already exists -- ignore
    conn.commit()


def save_dataset_relationship(conn, ind_id, num_id, den_id):
    try:
        c.execute("""
        INSERT INTO dataset_relationships
        (indicatore, numeratore, denominatore)
        VALUES (?, ?, ?)
        """, (ind_id, num_id, den_id))
    except sqlite3.IntegrityError:
        pass  # already exists
    conn.commit()


def get_indicatore_data(item_id):
    """
    Get data for an "indicatore"
    """

    result = {'id': item_id}

    ## Fetch metadata for this dataset
    url = get_ind_metadata_url(item_id)
    response = requests.get(url)
    data = response.json()
    assert isinstance(data, dict)
    assert len(data.keys()) == 1
    meta_title = data.keys()[0]
    result['metadata'] = data[meta_title]

    ## Fetch data for this dataset
    url = get_ind_data_url(item_id)
    response = requests.get(url)
    data = response.json()
    assert isinstance(data, dict)
    assert len(data.keys()) == 1
    title = data.keys()[0]
    result['title'] = title
    result['data'] = data[title]

    return result


def get_table_data(name):
    """
    Get data for a "table"
    """

    result = {'id': name}

    ## Fetch metadata for this dataset
    url = get_tab_metadata_url(name)
    response = requests.get(url)
    data = response.json()
    assert isinstance(data, dict)
    assert len(data.keys()) == 1
    meta_title = data.keys()[0]
    result['metadata'] = data[meta_title]

    ## Fetch data for this dataset
    url = get_tab_data_url(name)
    response = requests.get(url)
    data = response.json()
    assert isinstance(data, dict)
    assert len(data.keys()) == 1
    title = data.keys()[0]
    result['title'] = title
    result['data'] = data[title]

    return result


def prepare_row(data):
    # First of all, let's serialize stuff
    for k in ('data', 'metadata'):
        data[k] = json.dumps(data[k])

    data['metadata_hash'] = hashlib.sha1(data['metadata']).hexdigest()
    data['data_hash'] = hashlib.sha1(data['data']).hexdigest()
    return data

######################################################################

conn = sqlite3.connect(DATABASE)
create_tables(conn)

## Create the "index" table
print("Downloading main index")
response = requests.get(INDEX_URL)
data = response.json()
raw_items = data['Lista indicatori strutturali SP']
c = conn.cursor()

assert isinstance(raw_items, list)

for item in raw_items:
    assert isinstance(item, dict)

    ## We just want the IDs of linked objects
    item_id = int(item['id'])
    num_id = item['NomeTabNum']
    den_id = item['NomeTabDen']

    assert isinstance(num_id, unicode)
    assert isinstance(den_id, unicode)

    print(u"Fetching data for dataset {0}".format(item_id))

    print(u"    Saving relationship: ({0!r}, {1!r}, {2!r})"
          .format(item_id, num_id, den_id)
          .encode('utf-8'))
    save_dataset_relationship(conn, item_id, num_id, den_id)

    print(u"    Downloading indicatore")
    indicatore = get_indicatore_data(item_id)
    prepare_row(indicatore)
    save_dataset(conn, indicatore)

    for rel_id in (num_id, den_id):
        print(u"    Downloading related object: {0}".format(rel_id))
        if not rel_id:
            continue
        rel_obj = get_table_data(rel_id)
        save_dataset(conn, prepare_row(rel_obj))

conn.close()
