from flask import Blueprint, current_app
from flask.ext import restful
# from werkzeug.exceptions import NotFound

from harvester.ext.storage.mongodb import MongodbStorage


api_bp = Blueprint('api', __name__)
api = restful.Api(api_bp)


# ---- NOTES -----------------------------------------------------------
#   - we use a fixed storage bucket to hold configurations
#   - we use a fixed storage bucket to hold job execution state
#   - we use generated storage buckets for job storage
#   - storages are created as sub-collections of the configured storage
# ----------------------------------------------------------------------


def _get_storage(coll_name):
    storage_conf = dict(current_app.config['HARVESTER_MONGODB'])
    db_url = storage_conf.pop('host')
    db_name = storage_conf.pop('name')
    return MongodbStorage('/'.join((db_url, db_name, coll_name)), storage_conf)


class SourceList(restful.Resource):
    def get(self):
        storage = _get_storage('conf')
        return list(storage.documents['source'])


class Source(restful.Resource):
    def get(self, objid):
        return {'hello': objid}


class DestinationList(restful.Resource):
    def get(self):
        return {'hello': None}


class Destination(restful.Resource):
    def get(self, objid):
        return {'hello': objid}


class JobList(restful.Resource):
    def get(self):
        return {'hello': None}


class Job(restful.Resource):
    def get(self, objid):
        # Four kind of jobs: crawl, convert, preview, import
        return {'hello': objid}


api.add_resource(SourceList, '/sources/')
api.add_resource(Source, '/sources/<objid>/')
api.add_resource(DestinationList, '/destinations/')
api.add_resource(Destination, '/destinations/<objid>/')
api.add_resource(JobList, '/jobs/')
api.add_resource(Job, '/jobs/<objid>/')

# todo: we want to return a 404 w/ json payload on all missing endpoints
