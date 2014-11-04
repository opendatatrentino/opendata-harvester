from __future__ import division

from math import ceil

from flask import (Blueprint, render_template, redirect, url_for,
                   flash, request, session)

from harvester.utils import get_storage_direct
import json


html_views = Blueprint('webui', __name__)


DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def get_jc():
    from flask import current_app
    return current_app.config['JOBCONTROL']


class Paged(object):
    def __init__(self, data, page=1, per_page=10):
        """
        :param data: A dict-like to paginate
        :param page: The page number (one-based)
        """
        self._data = data
        self._page = page
        self._per_page = per_page

    @property
    def pages(self):
        return int(ceil(len(self._data) / float(self._per_page)))

    @property
    def current_page(self):
        return self._page

    @property
    def has_prev(self):
        return self._page > 1

    @property
    def has_next(self):
        return self._page < self.pages

    def get_items(self):
        start = (self._page - 1) * self._per_page
        end = start + self._per_page
        return self._data[start:end]

    def get_pager_links(self):
        page_ids = set()
        page_ids.add(1)
        page_ids.add(self.pages)

        range_start = max(1, self.current_page - 3)
        range_end = min(self.pages, self.current_page + 3)
        for i in xrange(range_start, range_end + 1):
            page_ids.add(i)

        _previtem = None
        for x in sorted(page_ids):
            if _previtem is not None and _previtem < x - 1:
                yield ('...', None)
            yield (str(x), x)
            _previtem = x


@html_views.route('/', methods=['GET'])
def index():
    return render_template('storage-url-form.jinja',
                           bookmarks=session.get('bookmarked_urls'))


@html_views.route('/', methods=['POST'])
def storage_connect():
    url = request.form['storage_url']

    if request.form.get('bookmark'):
        if 'bookmarked_urls' not in session:
            session['bookmarked_urls'] = []
        if url not in session['bookmarked_urls']:
            session['bookmarked_urls'].append(url)
        flash('Storage URL bookmarked: {0}'.format(url), 'success')

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
    bucket = storage.documents[bucket_name]
    return render_template(
        'storage-documents-index.jinja',
        storage=storage,
        bucket_name=bucket_name,
        bucket=bucket,
        paged_ids=Paged(
            bucket.keys(),
            page=int(request.args.get('page', 1)),
            per_page=50))


@html_views.route(
    '/storage/<quotedstring:storage_url>/blobs/<quotedstring:bucket_name>',
    methods=['GET'])
def storage_blobs_index(storage_url, bucket_name):
    storage = get_storage_direct(storage_url)
    bucket = storage.blobs[bucket_name]
    return render_template(
        'storage-blobs-index.jinja',
        storage=storage,
        bucket_name=bucket_name,
        bucket=bucket,
        paged_ids=Paged(
            bucket.keys(),
            page=int(request.args.get('page', 1)),
            per_page=50))


@html_views.route(
    '/storage/<quotedstring:storage_url>/keyvals/<quotedstring:bucket_name>',
    methods=['GET'])
def storage_keyvals_index(storage_url, bucket_name):
    storage = get_storage_direct(storage_url)
    bucket = storage.keyvals[bucket_name]
    return render_template(
        'storage-keyvals-index.jinja',
        storage=storage,
        bucket_name=bucket_name,
        bucket=bucket,
        paged_ids=Paged(
            bucket.keys(),
            page=int(request.args.get('page', 1)),
            per_page=50))


@html_views.route(
    '/storage/<quotedstring:storage_url>/documents/<quotedstring:bucket_name>'
    '/<typedstring:object_id>',
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
    '/storage/<quotedstring:storage_url>/blobs/<quotedstring:bucket_name>'
    '/<typedstring:object_id>',
    methods=['GET'])
def storage_blob_show(storage_url, bucket_name, object_id):
    storage = get_storage_direct(storage_url)
    return render_template(
        'storage-blob-show.jinja',
        storage=storage,
        bucket_name=bucket_name,
        object_id=object_id,
        obj=storage.blobs[bucket_name][object_id])


@html_views.route(
    '/storage/<quotedstring:storage_url>/keyvals/<quotedstring:bucket_name>'
    '/<typedstring:object_id>',
    methods=['GET'])
def storage_keyval_show(storage_url, bucket_name, object_id):
    storage = get_storage_direct(storage_url)
    return render_template(
        'storage-keyval-show.jinja',
        storage=storage,
        bucket_name=bucket_name,
        object_id=object_id,
        obj=storage.keyvals[bucket_name][object_id])


@html_views.route(
    '/storage/<quotedstring:storage_url>/documents/<quotedstring:bucket_name>'
    '/<typedstring:object_id>/download',
    methods=['GET'])
def storage_document_download(storage_url, bucket_name, object_id):
    storage = get_storage_direct(storage_url)
    obj = storage.documents[bucket_name][object_id]
    headers = {'Content-type': 'application/json'}
    return json.dumps(obj), 200, headers


@html_views.route(
    '/storage/<quotedstring:storage_url>/blobs/<quotedstring:bucket_name>'
    '/<typedstring:object_id>/download',
    methods=['GET'])
def storage_blob_download(storage_url, bucket_name, object_id):
    storage = get_storage_direct(storage_url)
    obj = storage.blobs[bucket_name][object_id]
    headers = {'Content-type': 'application/octet-stream',
               'Content-disposition': 'attachment; filename="blob-{0}-{1}"'
               .format(bucket_name, object_id)}
    return obj, 200, headers


@html_views.route(
    '/storage/<quotedstring:storage_url>/keyvals/<quotedstring:bucket_name>'
    '/<typedstring:object_id>/download',
    methods=['GET'])
def storage_keyval_download(storage_url, bucket_name, object_id):
    storage = get_storage_direct(storage_url)
    obj = storage.keyvals[bucket_name][object_id]
    headers = {'Content-type': 'application/json'}
    return json.dumps(obj), 200, headers
