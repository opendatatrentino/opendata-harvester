#!/bin/bash

for ENVID in "$@"; do

echo "Runnign tests for $ENVID"

MONGO_HOST=database.local

TEST_DB_PREFIX="$( date '+%s' )"
TEST_DB_HARVESTER="test_${TEST_DB_PREFIX}_harvester"
TEST_DB_DIRECTOR="test_${TEST_DB_PREFIX}_director"

export MONGO_URL=mongodb://"$MONGO_HOST":27017/"${TEST_DB_HARVESTER}"/testdata
export HARVESTER_SETTINGS="$( tempfile -s .py )"

# Generate test settings
# ----------------------------------------

cat > "$HARVESTER_SETTINGS" <<EOF
from harvester.director.web.settings.development import *

DEBUG = True
HARVESTER_MONGODB = {
    'host': 'mongodb://${MONGO_HOST}:27017',
    'name': '${TEST_DB_DIRECTOR}',
}
CELERY_BROKER_URL = 'redis://localhost:6379'
CELERY_RESULT_BACKEND = 'redis://localhost:6379'
EOF

# Run tests
# ----------------------------------------
tox -e "$ENVID"

# Cleanup
# ----------------------------------------
echo "Cleanup.."
rm "$HARVESTER_SETTINGS"
echo -e 'use ${TEST_DB_HARVESTER}\ndb.dropDatabase()' | mongo "$MONGO_HOST" &>/dev/null
echo -e 'use ${TEST_DB_DIRECTOR}\ndb.dropDatabase()' | mongo "$MONGO_HOST" &>/dev/null

done
