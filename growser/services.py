import importlib
import logging.config
import ujson as json

from celery import Celery
from flask_sqlalchemy import BaseQuery

from growser.db import SQLAlchemyAutoCommit, to_dict_model, to_dict_query
from growser.cmdr import Registry, LocalCommandBus
from growser.google import BigQueryService, CloudStorageService


def bigquery(app):
    return __google_service(app, BigQueryService)


def storage(app):
    return __google_service(app, CloudStorageService)


def __google_service(app, klass):
    with open(app.config.get('GOOGLE_CLIENT_KEY')) as fh:
        config = json.loads(fh.read())

    project_id = app.config.get('GOOGLE_PROJECT_ID')
    account = config['client_email']
    private_key = bytes(config['private_key'], 'UTF-8')

    return klass(project_id, account, private_key)


def celery(app):
    rv = Celery('tasks')
    rv.conf.update(app.config)
    return rv


def log(app, cfg=None):
    app.debug = app.logger.name and app.config.get('DEBUG', False)
    if cfg:
        logging.config.fileConfig(cfg)
    return app.logger


def commands(app):
    handlers = Registry()
    for module in app.config.get('CMDR_HANDLERS'):
        handlers.scan(importlib.import_module(module))
    return LocalCommandBus(handlers)


def sqlalchemy(app):
    db = SQLAlchemyAutoCommit(app)
    db.Model.to_dict = to_dict_model
    BaseQuery.to_dict = to_dict_query
    return db
