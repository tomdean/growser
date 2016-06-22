from growser.app import app, celery, log
from growser.services import commands


@celery.task()
def run_command(command):
    log.info("Executing command: {}".format(command))
    bus = commands(app)
    return bus.execute(command)
