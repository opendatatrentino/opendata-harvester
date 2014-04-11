# Download all the csv / json resources and print links of the "broken" ones

from __future__ import print_function, division

import csv
import json
import time
import datetime
from collections import defaultdict

import pymongo
import requests


def download(url):
    response = requests.get(url)
    assert response.ok
    return response.text


def check_csv(data):
    reader = csv.reader(data.splitlines(), delimiter=';')
    ncolumns = len(reader.next())
    for row in reader:
        if len(row) != ncolumns:
            return False
    return True


def check_json(data):
    try:
        json.loads(data)
    except:
        return False
    return True


def find_datasets():
    conn = pymongo.MongoClient('database.local')
    for d in conn['harvester_data']['statistica_clean.dataset'].find():
        yield d
    for d in conn['harvester_data']['statistica_subpro_clean.dataset'].find():
        yield d


def check_resources(resources, checker):
    cnt = len(resources)
    start_time = time.time()
    for i, url in enumerate(resources):
        try:
            data = download(url)
        except:
            print("\x1b[1;31mDownload failed:\x1b[0m {0}".format(url))
        else:
            if not checker(data):
                print("\x1b[1;33mCheck failed:\x1b[0m {0}".format(url))

        n = i + 1
        pc = n * 100.0 / cnt
        elapsed = time.time() - start_time
        eta = (elapsed * cnt / n) - elapsed
        eta = str(datetime.timedelta(seconds=int(eta)))
        elapsed = str(datetime.timedelta(seconds=int(elapsed)))
        print("\x1b[K\x1b[1;32mProcessed:\x1b[0m {done}/{total} ({pc:.1f}%) "
              "\x1b[1;32mElapsed:\x1b[0m {elapsed} "
              "\x1b[1;32mETA:\x1b[0m {eta}\n\x1b[F"
              .format(done=n, total=cnt, pc=pc, eta=eta, elapsed=elapsed),
              end='')
    print()


def split_resources(datasets):
    resources = defaultdict(list)
    for row in datasets:
        for resource in row['resources']:
            resources[resource['format'].lower()].append(resource['url'])
    return resources


resources = {}

conn = pymongo.MongoClient('database.local')
db = conn['harvester_data']

resources['statistica'] = split_resources(
    db['statistica_clean.dataset'].find())
resources['statistica_subpro'] = split_resources(
    db['statistica_subpro_clean.dataset'].find())

# --- print summary
for k1 in sorted(resources.keys()):
    for k2 in sorted(resources[k1].keys()):
        print("{0} has {1} {2} datasets"
              .format(k1, len(resources[k1][k2]), k2))
print()

# --- check all files
for source_id in sorted(resources.keys()):
    for type_id in sorted(resources[source_id].keys()):
        print("\x1b[1mChecking {0} datasets in {1}\x1b[0m"
              .format(type_id, source_id))
        if type_id == 'csv':
            check_resources(resources[source_id][type_id], check_csv)

        elif type_id == 'json':
            check_resources(resources[source_id][type_id], check_json)

        else:
            print("No checker for this type!!")

        print()
