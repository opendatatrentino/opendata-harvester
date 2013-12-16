"""

Build a GEXF graph from relationships between datasets

"""

from __future__ import print_function

import os
import sqlite3
import sys

import networkx


HERE = os.path.abspath(os.path.dirname(__file__))
DATABASE = os.path.join(HERE, 'statistica.db')

conn = sqlite3.connect(DATABASE)

fields = ['indicatore', 'numeratore', 'denominatore']
all_fields = ['id']
for fld in fields:
    all_fields.extend([fld + '_id', fld + '_hash'])

nodes = {}  # id : label
edges = set()  # (from, to, name)


cur = conn.cursor()
query = "SELECT {fields} FROM datasets".format(fields=', '.join(all_fields))
cur.execute(query)
rows = cur.fetchall()

for raw_row in rows:
    row = dict(zip(all_fields, raw_row))

    for fld in fields:
        node_id = row[fld + '_hash']
        if not node_id:
            continue
        node_label = row[fld + '_id']
        nodes[node_id] = node_label

    if row['numeratore_hash']:
        edge_num = (row['indicatore_hash'], row['numeratore_hash'], 'num')
        edges.add(edge_num)

    if row['denominatore_hash']:
        edge_den = (row['indicatore_hash'], row['denominatore_hash'], 'den')
        edges.add(edge_den)

conn.close()

## --- Now dump the data ---


G = networkx.DiGraph()
for node_id, node_label in nodes.iteritems():
    G.add_node(node_id, label=node_label)
for efrom, eto, elabel in edges:
    G.add_edge(efrom, eto, label=elabel)

networkx.write_gexf(G, sys.stdout)
