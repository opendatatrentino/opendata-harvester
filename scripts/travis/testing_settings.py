from harvester.director.web.settings.development import *  # noqa


DEBUG = True
HARVESTER_MONGODB = {
    'host': 'mongodb://localhost:27017',
    'name': 'test_harvester_director',
}
CELERY_BROKER_URL = 'redis://localhost:6379'
CELERY_RESULT_BACKEND = 'redis://localhost:6379'
