from growser.app import celery, log
from growser.media import score_image


@celery.task()
def score_images(filenames):
    log.info("Processing batch: {}".format(len(filenames)))
    rv = []
    for filename in filenames:
        rv.append((filename, *score_image(filename)))
    return rv
