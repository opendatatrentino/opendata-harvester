import cgi
import json

from flask import Blueprint, current_app, request
from flask.ext import restful
from werkzeug.exceptions import NotFound, BadRequest

from harvester.ext.storage.base import NotFound as StorageObjNotFound
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


def _get_request_object():
    """
    Get object from request.
    """
    ctype, ctypeopts = cgi.parse_header(request.content_type)

    if ctype == 'application/json':
        rq_enc = ctypeopts.get('charset', 'utf-8')
        return json.loads(request.data.decode(rq_enc))

    if ctype == 'application/x-www-form-urlencoded':
        return request.form

    raise BadRequest('Unsupported request content type: {0}'.format(ctype))


class SourceList(restful.Resource):
    def get(self):
        storage = _get_storage('conf')
        all_objects = storage.documents['source'].iteritems()
        return [
            {'id': key,
             'label': obj.get('label'),
             'url': obj.get('url'),
             'options': obj.get('options')}
            for key, obj in all_objects]


class Source(restful.Resource):
    """
    A source is used to hold configuration for the crawler plugin.

    Fields:

    - id -> generated id
    - plugin_name -> choose which crawler plugin to use
    - url -> url of the crawler
    - options -> dictionary of options to be passed to the crawler
    """

    def get(self, objid):
        storage = _get_storage('conf')
        try:
            obj = storage.documents['source'][objid]
        except StorageObjNotFound:
            raise NotFound('Object not found')
        return {'id': objid,
                'label': obj.get('label'),
                'url': obj.get('url'),
                'options': obj.get('options')}

    def post(self, objid):
        storage = _get_storage('conf')
        rqob = _get_request_object()

        new_obj = {
            'label': rqob.get('label'),
            'url': rqob.get('url'),
            'options': rqob.get('options') or {},
        }

        if not isinstance(new_obj['options'], dict):
            raise BadRequest('Options must be a dict')

        storage.documents['source'][objid] = new_obj

        obj = storage.documents['source'][objid]
        obj['id'] = objid

        return obj, 201  # todo: return 201 for creation only

    def put(self, objid):
        return self.post(objid)

    def delete(self, objid):
        storage = _get_storage('conf')
        del storage.documents['source'][objid]


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
