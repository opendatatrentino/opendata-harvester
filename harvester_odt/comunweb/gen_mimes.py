#!/usr/bin/env python

import json
import sys


with open('/etc/mime.types') as fp:
    data = list(fp)

lines = filter(lambda x: len(x) >= 2, [d.split('#')[0].split() for d in data])
mimetypes = {}
for line in lines:
    for ext in line[1:]:
        mimetypes[ext.lower()] = line[0]

json.dump(mimetypes, sys.stdout)
