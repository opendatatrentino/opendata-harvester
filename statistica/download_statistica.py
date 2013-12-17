"""

Download data from statweb.provincia.tn.it into a SQLite database

"""

from __future__ import print_function

import hashlib
import json
import os
import sqlite3

import requests


HERE = os.path.abspath(os.path.dirname(__file__))
DATABASE = os.path.join(HERE, 'statistica.db')

# http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx?fmt=json

BASE_URL = 'http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx'
INDEX_URL = BASE_URL + "?fmt=json"
ds_urls = {
    "metadata": BASE_URL + '?fmt=json&idind={id}',
    "indicatore": BASE_URL + '?fmt=json&idind={id}&t=i',
    "numeratore": BASE_URL + '?fmt=json&idind={id}&t=n',
    "denominatore": BASE_URL + '?fmt=json&idind={id}&t=d',
}


def create_tables(conn):
    c = conn.cursor()
    # c.execute("""
    # CREATE TABLE IF NOT EXISTS main_index
    # ( id INT PRIMARY KEY,
    #   url TEXT,
    #   title TEXT );
    # """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS datasets
    ( id INT PRIMARY KEY,

      metadata_id TEXT,
      metadata TEXT,
      metadata_hash TEXT,

      indicatore_id TEXT,
      indicatore TEXT,
      indicatore_hash TEXT,

      numeratore_id TEXT,
      numeratore TEXT,
      numeratore_hash TEXT,

      denominatore_id TEXT,
      denominatore TEXT,
      denominatore_hash TEXT
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS errors
    (id INT PRIMARY KEY,
     message TEXT);
    """)
    conn.commit()


def log_error(conn, message):
    c = conn.cursor()
    c.execute("INSERT INTO errors (message) "
              "VALUES (?)", (message,))
    conn.commit()


conn = sqlite3.connect(DATABASE)

create_tables(conn)

## Create the "index" table
print("Downloading main index")
response = requests.get(INDEX_URL)
data = response.json()
raw_items = data['IndicatoriStrutturali']
c = conn.cursor()

item_ids = set(int(x['id']) for x in raw_items)


for item_id in item_ids:
    print("Fetching data for dataset {0}".format(item_id))

    fields = ['id']
    values = [item_id]

    for name in ds_urls:
        print("    * Fetching {0}".format(name), end=' ')

        url = ds_urls[name].format(id=item_id)
        response = requests.get(url)

        print("({0} btyes)".format(len(response.text)), end=' ')

        if len(response.text) > 0:
            try:
                # Get the JSON payload
                payload = response.json()

            except:
                log_error(conn, 'Error parsing json in dataset {0}'
                          ''.format(item_id))
            else:
                assert isinstance(payload, dict)
                assert len(payload) == 1

                # The first (and only) key is the title
                title = payload.keys()[0]
                assert isinstance(payload[title], list)
                print(title, end=' ')

                # The rest is the actual content
                metadata = payload[title]
                if name == 'metadata':
                    assert isinstance(metadata, list)
                    #assert len(metadata) == 1
                    if len(metadata) != 1:
                        log_error(conn, "Dataset {0} has {1} values".format(
                            item_id, len(metadata)))
                    try:
                        metadata = metadata[0]
                    except IndexError:
                        metadata = {}

                json_text = json.dumps(metadata)
                json_text_hash = hashlib.sha1(json_text).hexdigest()

                fields.extend([name + '_id', name, name + '_hash'])
                values.extend([title, json_text, json_text_hash])

        else:
            log_error(
                conn, 'Missing {0} for dataset {1}'.format(name, item_id))

        print("")

    ## Store the record
    query = "INSERT INTO datasets ({fields}) VALUES ({values});".format(
        fields=', '.join(fields), values=', '.join('?' for x in values))
    c.execute(query, values)
    conn.commit()

conn.close()
