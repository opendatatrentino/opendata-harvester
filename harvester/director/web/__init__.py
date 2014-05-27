from flask import Flask
from .webapi import api_bp
from .webui import web_bp

app = Flask(__name__)

app.config.from_object('harvester.director.web.settings.default')
app.config.from_envvar('HARVESTER_SETTINGS', silent=True)  # path to file

app.register_blueprint(api_bp, url_prefix='/api/1')
app.register_blueprint(web_bp, url_prefix='')
