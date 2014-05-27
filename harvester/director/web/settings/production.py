from harvester.director.web.settings.default import *  # noqa


# Make sure debug is off for production
DEBUG = False
TESTING = False
PRESERVE_CONTEXT_ON_EXCEPTION = False

SECRET_KEY = None  # Must be set in custom settings
SERVER_NAME = None  # Should be set in custom settings

HARVESTER_MONGODB = {
    'host': 'mongodb://localhost:27017',
    'name': 'harvester_prod',
}
