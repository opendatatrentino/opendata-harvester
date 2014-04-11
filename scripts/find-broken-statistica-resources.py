# Download all the csv / json resources and print links of the "broken" ones

from __future__ import print_function

import csv
import json

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
    for i, url in enumerate(resources):
        try:
            data = download(url)
        except:
            print("Download failed: {0}".format(url))
        else:
            if not checker(data):
                print("Check failed: {0}".format(url))
                print(url)
        print("\x1b[K    processed: {0}/{1} \n\x1b[F"
              .format(i, cnt), end='')
    print()


csv_resources = []
json_resources = []

for row in find_datasets():
    for resource in row['resources']:
        if resource['format'] == 'JSON':
            json_resources.append(resource['url'])
        elif resource['format'] == 'CSV':
            csv_resources.append(resource['url'])

print("We have {0} csv resources".format(len(csv_resources)))
print("We have {0} json resources".format(len(json_resources)))
print()

print("Looking for broken CSV files")
check_resources(csv_resources, check_csv)

print("Looking for broken JSON files")
check_resources(csv_resources, check_json)
