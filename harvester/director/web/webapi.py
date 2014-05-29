import cgi
import json
import time
import uuid

from flask import Blueprint, request
from flask.ext import restful
from werkzeug.exceptions import NotFound, BadRequest

from harvester.ext.storage.base import NotFound as StorageObjNotFound
from harvester.director import HarvesterDirector


api_bp = Blueprint('api', __name__)
api = restful.Api(api_bp)


# ---- NOTES -----------------------------------------------------------
#   - we use a fixed storage bucket to hold configurations
#   - we use a fixed storage bucket to hold job execution state
#   - we use generated storage buckets for job storage
#   - storages are created as sub-collections of the configured storage
# ----------------------------------------------------------------------


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
        hd = HarvesterDirector()
        storage = hd.get_storage('conf')
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
        hd = HarvesterDirector()
        storage = hd.get_storage('conf')
        try:
            obj = storage.documents[self.bucket_name][objid]
        except StorageObjNotFound:
            raise NotFound('Object not found')
        return {'id': objid,
                'label': obj.get('label'),
                'url': obj.get('url'),
                'options': obj.get('options')}

    def post(self, objid):
        hd = HarvesterDirector()
        storage = hd.get_storage('conf')
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
        hd = HarvesterDirector()
        storage = hd.get_storage('conf')
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
        hd = HarvesterDirector()
        rqobj = _get_request_object()
        task = self._prepare_task(rqobj)
        hd.run_job(task)

    def _prepare_task(self, rqobj):
        if rqobj['type'] == 'crawl':
            task = self._prepare_task_crawl(rqobj)

        if rqobj['type'] == 'convert':
            task = self._prepare_task_convert(rqobj)

        if rqobj['type'] == 'import':
            task = self._prepare_task_import(rqobj)

        if rqobj['type'] == 'preview':
            task = self._prepare_task_preview(rqobj)

        else:
            raise ValueError("Invalid task type: {0}".format(rqobj['type']))

        task.update({
            'id': 'job-{0}-{1}'.format(rqobj['type'], str(uuid.uuid4())),
            'type': rqobj['type'],
            'start_time': time.time(),
            'end_time': None,
            'started': False,
            'finished': False,
            'result': None,  # True | False
        })
        return task

    def _prepare_task_crawl(self, rqobj):
        return {
            'crawler': self._get_crawler_conf(rqobj),
            'output_storage': self._get_output_storage_conf(rqobj),
        }

    def _prepare_task_convert(self, rqobj):
        return {
            'converter': self._get_converter_conf(rqobj),
            'input_storage': self._get_input_storage_conf(rqobj),
            'output_storage': self._get_output_storage_conf(rqobj),
        }

    def _prepare_task_import(self, rqobj):
        return {
            'importer': self._get_converter_conf(rqobj),
            'input_storage': self._get_input_storage_conf(rqobj),
        }

    def _prepare_task_preview(self, rqobj):
        return {
            'importer': self._get_converter_conf(rqobj),
            'input_storage': self._get_input_storage_conf(rqobj),
            'output_storage': self._get_output_storage_conf(rqobj),
        }

    def _get_crawler_conf(self, rqobj):
        return self._get_object_conf('crawler')

    def _get_converter_conf(self, rqobj):
        return self._get_object_conf('converter')

    def _get_importer_conf(self, rqobj):
        return self._get_object_conf('importer')

    def _get_object_conf(self, rqobj, objtype):
        hd = HarvesterDirector()

        if objtype in rqobj:
            conf_name = rqobj[objtype]
            conf_storage = hd.get_storage('conf')
            conf = conf_storage.documents[objtype][conf_name]
            url = conf['url']
            opts = conf.get('options') or {}
            return {'url': url, 'options': opts, 'id': conf_name}

        elif objtype + '_url' in rqobj:
            url = rqobj[objtype + '_url']
            opts = rqobj.get(objtype + '_options') or {}
            return {'url': url, 'options': opts}

        raise ValueError(
            "You must specify either a ``{0}`` or ``{0}_url``"
            .format(objtype))

    def _get_input_storage_conf(self, rqobj):
        if 'input_storage' in rqobj:
            hd = HarvesterDirector()
            if not rqobj['input_storage']:
                raise ValueError("Storage name cannot be empty")
            storage_name = rqobj['output_storage']
            if not storage_name.startswith('job.'):
                storage_name = 'job.' + storage_name
            url, opts = hd.get_storage_url_options(storage_name)
            return {'url': url, 'options': opts}

        raise ValueError("An input storage name is required")

    def _get_output_storage_conf(self, rqobj):
        if 'output_storage' in rqobj:
            if not rqobj['output_storage']:
                raise ValueError("Storage name cannot be empty")
            storage_name = rqobj['output_storage']
            if not storage_name.startswith('job.'):
                storage_name = 'job.' + storage_name

        else:
            # Generate a new storage name
            storage_name = 'job.{0}'.format(str(uuid.uuid4()))

        hd = HarvesterDirector()
        url, opts = hd.get_storage_url_options(storage_name)
        return {'url': url, 'options': opts}

    def _generate_storage_name(self):
        return 'job.{0}'.format(str(uuid.uuid4()))


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
