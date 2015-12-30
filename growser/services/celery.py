from celery import Celery


def configure(app):
    celery = Celery('growser-tasks')
    celery.conf.update(app.config)
    return celery
