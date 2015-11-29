import json
import logging.config

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from growser.services.google import BigQueryService, CloudStorageService


app = Flask(__name__, template_folder="../templates", static_folder="../static")
app.config.from_object('growser.config.BasicConfig')
app.config.from_envvar('GROWSER_CONFIG', False)
app.debug = app.logger.name and False
logging.config.fileConfig("logging.cfg")

db = SQLAlchemy(app)

with open(app.config.get('GOOGLE_CLIENT_KEY')) as fh:
    js = json.loads(fh.read())

bigquery = BigQueryService(app.config.get('GOOGLE_PROJECT_ID'),
                           js['client_email'],
                           bytes(js['private_key'], 'UTF-8'))

storage = CloudStorageService(app.config.get('GOOGLE_PROJECT_ID'),
                              js['client_email'],
                              bytes(js['private_key'], 'UTF-8'))
