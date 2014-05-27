from harvester.director.web.settings.default import *  # noqa


DEBUG = True
SECRET_KEY = 'Just a dummy secret key'
# SERVER_NAME = 'localhost:5000'

HARVESTER_MONGODB = {
    'host': 'mongodb://localhost:27017',
    'name': 'harvester_dev',
}
