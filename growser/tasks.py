from PIL import Image

from growser.app import app, celery, log
from growser.media import score_image
from growser.services import commands


@celery.task()
def run_command(command):
    log.info("Executing command: {}".format(command))
    bus = commands(app)
    return bus.execute(command)


@celery.task()
def score_images(filenames):
    log.info("Processing batch: {}".format(len(filenames)))
    rv = []
    for filename in filenames:
        rv.append((filename, *score_image(Image.open(filename))))
    return rv
