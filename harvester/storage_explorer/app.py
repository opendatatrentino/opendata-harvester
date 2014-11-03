from werkzeug.routing import BaseConverter

import urllib
import flask
# from flask import request, session, abort

from .views_html import html_views
# from jobcontrol.utils.web import generate_csrf_token
# from jobcontrol.web.template_filters import filters


app = flask.Flask('harvester.storage_explorer')


class QuotedStringConverter(BaseConverter):
    regex = '[^/]+'

    def __init__(self, *a, **kw):
        super(QuotedStringConverter, self).__init__(*a, **kw)
        self.regex = '[^/].*?'

    def to_python(self, value):
        return urllib.unquote(value)

    def to_url(self, value):
        return urllib.quote(value, safe='')

app.url_map.converters['quotedstring'] = QuotedStringConverter


# app.register_blueprint(api_views, url_prefix='/api/1')
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
