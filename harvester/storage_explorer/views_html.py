from __future__ import division

from flask import (Blueprint, render_template, redirect, url_for,
                   flash, request)

from harvester.utils import get_storage_direct
import json

# from jobcontrol.utils import import_object


class FormError(Exception):
    pass


html_views = Blueprint('webui', __name__)


DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def get_jc():
    from flask import current_app
    return current_app.config['JOBCONTROL']


@html_views.route('/', methods=['GET'])
def index():
    return render_template('storage-url-form.jinja')


@html_views.route('/', methods=['POST'])
def storage_connect():
    url = request.form['storage_url']
    return redirect(url_for(
        '.storage_index', storage_url=url))


@html_views.route('/storage/<quotedstring:storage_url>', methods=['GET'])
def storage_index(storage_url):
    storage = get_storage_direct(storage_url)
    return render_template('storage-index.jinja', storage=storage)


@html_views.route(
    '/storage/<quotedstring:storage_url>/documents/<quotedstring:bucket_name>',
    methods=['GET'])
def storage_documents_index(storage_url, bucket_name):
    storage = get_storage_direct(storage_url)
    return render_template(
        'storage-documents-index.jinja',
        storage=storage,
        bucket_name=bucket_name,
        bucket=storage.documents[bucket_name])


@html_views.route(
    '/storage/<quotedstring:storage_url>/blobs/<quotedstring:bucket_name>',
    methods=['GET'])
def storage_blobs_index(storage_url, bucket_name):
    storage = get_storage_direct(storage_url)
    return render_template(
        'storage-blobs-index.jinja',
        storage=storage,
        bucket_name=bucket_name,
        bucket=storage.blobs[bucket_name])


@html_views.route(
    '/storage/<quotedstring:storage_url>/keyvals/<quotedstring:bucket_name>',
    methods=['GET'])
def storage_keyvals_index(storage_url, bucket_name):
    storage = get_storage_direct(storage_url)
    return render_template(
        'storage-keyvals-index.jinja',
        storage=storage,
        bucket_name=bucket_name,
        bucket=storage.keyvals[bucket_name])


@html_views.route(
    '/storage/<quotedstring:storage_url>/documents/<quotedstring:bucket_name>'
    '/<quotedstring:object_id>',
    methods=['GET'])
def storage_document_show(storage_url, bucket_name, object_id):
    storage = get_storage_direct(storage_url)
    return render_template(
        'storage-document-show.jinja',
        storage=storage,
        bucket_name=bucket_name,
        object_id=object_id,
        obj=storage.documents[bucket_name][object_id])


@html_views.route(
    '/storage/<quotedstring:storage_url>/documents/<quotedstring:bucket_name>'
    '/<quotedstring:object_id>/download',
    methods=['GET'])
def storage_document_download(storage_url, bucket_name, object_id):
    storage = get_storage_direct(storage_url)
    obj = storage.documents[bucket_name][object_id]
    headers = {'Content-type': 'application/json'}
    return json.dumps(obj), 200, headers
