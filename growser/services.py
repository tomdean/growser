import importlib
import logging.config

from celery import Celery

from growser.db import SQLAlchemyAutoCommit, to_dict_model, to_dict_query
from growser.cmdr import HandlerRegistry, LocalCommandBus
from growser.google import BigQueryService, CloudStorageService


def bigquery(app):
    return BigQueryService(app.config.get('GOOGLE_PROJECT_ID'),
                           app.config.get('GOOGLE_CLIENT_KEY'))


def storage(app):
    return CloudStorageService(
        app.config.get('GOOGLE_PROJECT_ID'),
        app.config.get('GOOGLE_CLIENT_KEY')
    )


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
    handlers = HandlerRegistry()
    for module in app.config.get('CMDR_HANDLERS'):
        handlers.register(importlib.import_module(module))
    return LocalCommandBus(handlers)


def sqlalchemy(app):
    from flask_sqlalchemy import BaseQuery

    db = SQLAlchemyAutoCommit(app)
    db.Model.to_dict = to_dict_model
    BaseQuery.to_dict = to_dict_query
    return db
