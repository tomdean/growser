from celery import Celery


def configure(app):
    celery = Celery('tasks')
    celery.conf.update(app.config)
    return celery
