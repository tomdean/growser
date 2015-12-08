import json
import logging.config
import os

from celery import Celery
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from growser.services.google import BigQueryService, CloudStorageService


ROOT_PATH = os.path.realpath(__file__)

app = Flask(__name__, template_folder="../templates", static_folder="../static")
app.config.from_object('growser.config.BasicConfig')
app.config.from_envvar('GROWSER_CONFIG', False)
app.debug = app.logger.name and app.config.get('DEBUG', False)
logging.config.fileConfig("logging.cfg")


class SQLAlchemyAutoCommit(SQLAlchemy):
    """By default ``psycopg2`` will wrap SELECT statements in a transaction.

    This can be avoided using AUTOCOMMIT to rely on Postgres' default
    implicit transaction mode (see this `blog post <http://bit.ly/1N0a7Lj>`_
    for more details).
    """
    def apply_driver_hacks(self, app, info, options):
        super().apply_driver_hacks(app, info, options)
        options['isolation_level'] = 'AUTOCOMMIT'

db = SQLAlchemyAutoCommit(app)

with open(app.config.get('GOOGLE_CLIENT_KEY')) as fh:
    js = json.loads(fh.read())

bigquery = BigQueryService(app.config.get('GOOGLE_PROJECT_ID'),
                           js['client_email'],
                           bytes(js['private_key'], 'UTF-8'))

storage = CloudStorageService(app.config.get('GOOGLE_PROJECT_ID'),
                              js['client_email'],
                              bytes(js['private_key'], 'UTF-8'))

celery = Celery('tasks')
celery.conf.update(app.config)
