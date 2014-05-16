#!/usr/bin/env python

"""Extract tags from all the stored geocatalogo datasets"""

import pymongo
import lxml.etree

mongo = pymongo.MongoClient('database.local')
db = mongo['harvester-20140515-145800']

tags = set()
for dataset in db['pat_geocatalogo.document.dataset'].find():
    xml = lxml.etree.fromstring(dataset['raw_xml'])
    tags.update(
        # tx.decode('utf-8') for tx in
        xml.xpath('/csw:SummaryRecord/dc:subject/text()',
                  namespaces=xml.nsmap))

for tag in sorted(tags):
    print(tag.encode('utf-8'))
