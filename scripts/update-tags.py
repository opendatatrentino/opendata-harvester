import pprint

import pymongo
import lxml.etree

import harvester_odt.pat_geocatalogo.tags_map
from harvester_odt.pat_geocatalogo.tags_map import TAGS_MAP

# todo: check we're updating the correct file!!
tm_file = harvester_odt.pat_geocatalogo.tags_map.__file__
if tm_file.endswith('.pyc'):
    tm_file = tm_file[:-1]

mongo = pymongo.MongoClient('database.local')
db = mongo['harvester-20140515-145800']

tags = set()
for dataset in db['pat_geocatalogo.document.dataset'].find():
        xml = lxml.etree.fromstring(dataset['raw_xml'])
        tags.update(
            # tx.decode('utf-8') for tx in
            xml.xpath('/csw:SummaryRecord/dc:subject/text()',
                      namespaces=xml.nsmap))

for tag in tags:
    if tag not in TAGS_MAP:
        TAGS_MAP[tag] = None

# Now, write it back..

with open(tm_file, 'w') as fp:
    fp.write('# -*- coding: utf-8 -*-\n\n')
    fp.write('# flake8: noqa\n\n')
    fp.write('from __future__ import unicode_literals\n\n')
    fp.write('TAGS_MAP = ')
    fp.write(pprint.pformat(TAGS_MAP))
