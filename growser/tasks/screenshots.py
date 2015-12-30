from math import ceil
from PIL import Image
import re
import subprocess

import requests

from growser.app import celery, log


GITHUB_URLS = {
    "readme": "https://github.com/{name}/blob/master/README.md",
    "contrib": "https://github.com/{name}/graphs/contributors",
    "pulse": "https://github.com/{name}/pulse/monthly"
}

PHANTOM_JS_CMD = [
    "bin/phantomjs",
    "--disk-cache=true",
    "--max-disk-cache-size=10485760"
]


def image_path(hashid, page, folder):
    return "static/github/{folder}/{id}.{page}.png".format(
            id=hashid, page=page, folder=folder)


def update_repositories(repos):
    for repo in repos:
        update_repository_screenshots.delay(repo)


@celery.task
def update_repository_screenshots(repo):
    urls = GITHUB_URLS.copy()
    if repo.homepage:
        urls['hp'] = repo.homepage

    for page, url in urls.items():
        url = url.format(name=repo.name)
        if page == 'readme':
            url = _find_readme_url(repo.name)
            if not url:
                continue

        log.debug("Screenshot %s", url)

        full_size = image_path(repo.hashid, page, 'fs')
        thumbnail = image_path(repo.hashid, page, 'ts')
        subprocess.call(PHANTOM_JS_CMD + ["bin/screenshot.js", url, full_size])

        optimize_png.delay(full_size)
        generate_thumbnail.delay(full_size, thumbnail, (150, 225))


@celery.task
def optimize_png(source: str):
    if 'png' not in source:
        return False

    log.debug("Optimizing PNG %s", source)
    original = Image.open(source)
    optimized = original.convert('RGB').convert('P', palette=Image.ADAPTIVE)
    optimized.save(source)

    return True


@celery.task
def generate_thumbnail(source: str, destination: str, dimension: tuple):
    log.debug("Creating thumbnail from %s", source)
    img = Image.open(source).convert("RGB")
    bg = Image.new("RGB", img.size, (255, 255, 255))
    bg.paste(img, (0, 0))

    height = ceil(dimension[0] * img.size[1] / img.size[0])
    bg = bg.resize((dimension[0], height), Image.ANTIALIAS)
    bg = bg.crop((0, 0, dimension[0], dimension[1]))
    bg.save(destination, "JPEG", optimize=True, quality=95)


readme = re.compile('href="(/[^/]+/[^/]+/blob/[^/]+/readme(\.[^"]*)?)"',
                    re.IGNORECASE)


def _find_readme_url(name):
    """Find the URL of the projects README."""
    content = requests.get('https://github.com/' + name).content
    res = readme.findall(content.decode('utf-8'))
    if len(res):
        return "https://github.com" + res[0][0]
    return False
