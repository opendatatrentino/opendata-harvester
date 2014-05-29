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


class PluginConfResourceList(restful.Resource):
    bucket_name = None

    def get(self):
        storage = _get_storage('conf')
        all_objects = storage.documents[self.bucket_name].iteritems()
        return [
            {'id': key,
             'label': obj.get('label'),
             'url': obj.get('url'),
             'options': obj.get('options')}
            for key, obj in all_objects]


class PluginConfResource(restful.Resource):
    """
    Fields:

    - id -> generated id
    - plugin_name -> choose which crawler plugin to use
    - url -> url of the crawler
    - options -> dictionary of options to be passed to the crawler
    """

    bucket_name = None

    def get(self, objid):
        storage = _get_storage('conf')
        try:
            obj = storage.documents[self.bucket_name][objid]
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

        storage.documents[self.bucket_name][objid] = new_obj

        obj = storage.documents[self.bucket_name][objid]
        obj['id'] = objid

        return obj, 201  # todo: return 201 for creation only

    def put(self, objid):
        return self.post(objid)

    def delete(self, objid):
        storage = _get_storage('conf')
        del storage.documents[self.bucket_name][objid]


class CrawlerConfList(PluginConfResourceList):
    bucket_name = 'source'


class CrawlerConf(PluginConfResource):
    bucket_name = 'source'


class ImporterConfList(PluginConfResourceList):
    bucket_name = 'destination'


class ImporterConf(PluginConfResource):
    bucket_name = 'destination'


class ConverterConfList(PluginConfResourceList):
    bucket_name = 'converter'


class ConverterConf(PluginConfResource):
    bucket_name = 'converter'


class TaskListResource(restful.Resource):
    """
    Resource to list jobs (on GET) and trigger execution (on POST)
    """

    def get(self):
        # Return list of jobs.
        # todo: we need pagination support here..
        return {'hello': None}

    def post(self):
        rqobj = _get_request_object()

        # We want to create the job record
        # And then launch it via celery

        if rqobj['type'] == 'crawl':
            return self._run_task_crawl(rqobj)

        if rqobj['type'] == 'convert':
            return self._run_task_convert(rqobj)

        if rqobj['type'] == 'import':
            return self._run_task_import(rqobj)

        if rqobj['type'] == 'preview':
            return self._run_task_preview(rqobj)

    def _run_task_crawl(self, rqobj):
        # todo: create job record
        # todo: trigger execution via celery worker
        # todo: return information about the started job
        pass

    def _run_task_convert(self, rqobj):
        # todo: create job record
        # todo: trigger execution via celery worker
        # todo: return information about the started job
        pass

    def _run_task_import(self, rqobj):
        # todo: create job record
        # todo: trigger execution via celery worker
        # todo: return information about the started job
        pass

    def _run_task_preview(self, rqobj):
        # todo: create job record
        # todo: trigger execution via celery worker
        # todo: return information about the started job
        pass


class TaskResource(restful.Resource):
    """
    Resource used to get information about jobs.
    """

    def get(self, objid):
        # Four kind of jobs: crawl, convert, preview, import
        return {'hello': objid}


api.add_resource(CrawlerConfList, '/conf/crawler/')
api.add_resource(CrawlerConf, '/conf/crawler/<objid>/')
api.add_resource(ConverterConfList, '/conf/converter/')
api.add_resource(ConverterConf, '/conf/converter/<objid>/')
api.add_resource(ImporterConfList, '/conf/importer/')
api.add_resource(ImporterConf, '/conf/importer/<objid>/')
api.add_resource(TaskListResource, '/task/')
api.add_resource(TaskResource, '/task/<objid>/')

# todo: we want to return a 404 w/ json payload on all missing endpoints
