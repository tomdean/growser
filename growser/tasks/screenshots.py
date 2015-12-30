from math import ceil
import os
from PIL import Image
import re
import subprocess

import requests

from growser.app import celery, log


PHANTOM_JS_CMD = [
    "bin/phantomjs",
    "--disk-cache=true",
    "--max-disk-cache-size=10485760"
]

THUMBNAIL_DIMENSIONS = (150, 225)


def update_repositories(repos):
    for repo in repos:
        update_repository_screenshots.delay(repo)


@celery.task
def update_repository_screenshots(repo):
    urls = []
    if repo.homepage:
        urls.append(("hp", repo.homepage))

    readme_url = _find_readme_url(repo.name)
    if readme_url:
        urls.append(('readme', readme_url))

    if not len(urls):
        return False

    def image_path(hashid, page, folder):
        return "static/github/{folder}/{id}.{page}.png".format(
                id=hashid, page=page, folder=folder)

    for page, url in urls:
        log.info("Rendering %s", url)

        full_size = image_path(repo.hashid, page, 'fs')
        thumbnail = image_path(repo.hashid, page, 'ts')
        subprocess.call(PHANTOM_JS_CMD + ["bin/screenshot.js", url, full_size])

        generate_thumbnail(full_size, thumbnail, THUMBNAIL_DIMENSIONS)
        optimize_png(full_size)

    return True


def optimize_png(source: str):
    """Optimize PNG images from PhantomJS to reduce file sizes ~60-75%."""
    if 'png' not in source:
        return False

    log.info("Optimizing PNG %s", source)
    existing_size = os.path.getsize(source)

    original = Image.open(source)
    optimized = original.convert('P', palette=Image.ADAPTIVE)
    optimized.save(source)

    savings = existing_size - os.path.getsize(source)
    savings_pct = (savings / existing_size) * 100
    log.debug("Optimized PNG %s - %.2f%% saved (%dkB to %dkB)",
              source, savings_pct, existing_size / 1024,
              (existing_size - savings) / 1024)

    return True


def generate_thumbnail(source: str, destination: str, dimension: tuple):
    """Create a thumbnail from the full-size PNG.

    No default background is specified during the PhantomJS rendering step,
    resulting in a PNG with a transparent background. Pasting the screenshot
    onto a solid white image"""
    if not os.path.exists(source):
        log.warn('Source does not exist: %s', source)
        return False

    log.info("Creating thumbnail from %s", source)
    img = Image.open(source)
    height = ceil(dimension[0] * img.size[1] / img.size[0])
    img = img.resize((dimension[0], height), Image.ANTIALIAS)

    bg = Image.new("RGB", dimension, (255, 255, 255))
    bg.paste(img, (0, 0), img)
    bg.save(destination, "JPEG", optimize=True, quality=95)


readme = re.compile('href="(/[^/]+/[^/]+/blob/[^/]+/readme(\.[^"]*)?)"',
                    re.IGNORECASE)


def _find_readme_url(name):
    """Find the URL of the projects README."""
    content = requests.get('https://github.com/' + name).content
    res = readme.findall(content.decode('utf-8'))
    preferred = [x for x in res if x[1].lower() in ['.md', '.txt', '.rst']]
    if len(preferred):
        res = preferred
    if len(res):
        return "https://github.com" + res[0][0]
    return False
