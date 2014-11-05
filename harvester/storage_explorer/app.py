from werkzeug.routing import BaseConverter

import json
import urllib
import flask
# from flask import request, session, abort

from .views_html import html_views
# from jobcontrol.utils.web import generate_csrf_token
# from jobcontrol.web.template_filters import filters


app = flask.Flask('harvester.storage_explorer')


class QuotedStringConverter(BaseConverter):
    regex = '[^/].*?'

    def __init__(self, *a, **kw):
        super(QuotedStringConverter, self).__init__(*a, **kw)
        self.regex = '[^/].*?'

    def to_python(self, value):
        return urllib.unquote(value)

    def to_url(self, value):
        return urllib.quote(value, safe='')

app.url_map.converters['quotedstring'] = QuotedStringConverter


class TypedStringConverter(BaseConverter):
    regex = '[^/]+/[^/]+'

    def __init__(self, *a, **kw):
        super(TypedStringConverter, self).__init__(*a, **kw)

    def to_python(self, value):
        vtype, vval = value.split('/')
        unquoted = urllib.unquote(vval)

        if vtype == 's':
            return unicode(unquoted)

        if vtype == 'i':
            return int(vval)

        raise ValueError("Invalid type: {0}".format(vtype))

    def to_url(self, value):
        if isinstance(value, (basestring)):
            quoted = urllib.quote(value, safe='')
            return 's/{0}'.format(quoted)

        if isinstance(value, (int, long)):
            return 'i/{0}'.format(value)

        raise TypeError("Unsupported type: {0}".format(str(type(value))))


app.url_map.converters['typedstring'] = TypedStringConverter


# Register the Blueprint
app.register_blueprint(html_views, url_prefix='')


# IMPORTANT: This *must*  be set to something random
app.secret_key = "This is no secret"


# @app.before_request
# def csrf_protect():
#     if request.method == 'POST':
#         token = session.pop('_csrf_token', None)
#         if not token or token != request.form.get('_csrf_token'):
#             abort(403)


# app.jinja_env.globals['csrf_token'] = generate_csrf_token

# app.jinja_env.filters.update(filters)

def json_filter(obj, sort_keys=True, indent=4, **kw):
    kw['sort_keys'] = sort_keys
    kw['indent'] = indent
    return json.dumps(obj, **kw)

app.jinja_env.filters['json'] = json_filter


def format_blob_filter(obj):
    if '\x00' in obj:  # If contains a NULL, it's binary (grep strategy)
        return '[ Binary object not shown ]'

    if isinstance(obj, bytes):
        # todo: detect encoding
        return obj.decode('utf-8')

    return obj

app.jinja_env.filters['format_blob'] = format_blob_filter
