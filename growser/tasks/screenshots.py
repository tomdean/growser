from datetime import timedelta
from math import ceil
import os
from PIL import Image
import re
import subprocess

from sqlalchemy.sql import and_, exists, func, or_

from growser.app import celery, log
from growser.models import Repository, RepositoryTask
from growser import httpcache


#: Command to execute PhantomJS for rendering screenshots
PHANTOM_JS_CMD = ["bin/phantomjs",
                  "--ignore-ssl-errors=true",
                  "bin/screenshot.js"]

#: The width & height for generated thumbnails.
THUMBNAIL_DIMENSIONS = (('ts', (150, 225)), ('md', (400, 600)))


def update_eligible_repositories_screenshots(batch_size: int=500,
                                             require_homepage: bool=False):
    tasks = and_(RepositoryTask.name.in_(['screenshot.readme', 'screenshot.hp']),
                 RepositoryTask.repo_id == Repository.repo_id,
                 RepositoryTask.created_at >= func.now() - timedelta(days=30))

    query = Repository.query \
        .filter(Repository.status == 1) \
        .filter(or_(Repository.num_events >= 100, Repository.num_stars >= 100)) \
        .filter(~exists().where(tasks)) \
        .order_by(Repository.num_events.desc(), Repository.created_at.desc())

    if require_homepage:
        query = query.filter(Repository.homepage != '')

    repos = query.limit(batch_size).all()
    for repo in repos:
        update_repository_screenshots.delay(repo)


@celery.task
def update_repository_screenshots(repo: Repository):
    """Update the screenshots for a single repository.

    :param repo: The repository to update.
    """
    urls = []
    if repo.homepage:
        urls.append(("hp", repo.homepage))

    readme_url = _find_readme_url(repo.name)
    if readme_url:
        urls.append(('readme', readme_url))

    if not len(urls):
        return False

    def image_path(hashid, page, folder, ext):
        return "static/github/{folder}/{id}.{page}.{ext}".format(
                id=hashid, page=page, folder=folder, ext=ext)

    for name, url in urls:
        log.info("[%s] Rendering %s", repo.name, url)

        full_size = image_path(repo.hashid, name, 'fs', 'png')
        subprocess.call(PHANTOM_JS_CMD + [url, full_size])

        if not os.path.exists(full_size):
            log.warn('[%s] Failed to take screenshot: %s', repo.name, url)
            continue

        # Generate thumbnails prior to optimizing the PNG
        for path, size in THUMBNAIL_DIMENSIONS:
            thumb = image_path(repo.hashid, name, path, 'jpg')
            log.info("[%s] Creating thumbnail", repo.name)
            generate_thumbnail(full_size, thumb, size)

        # Reduce the quality of the full size image to conserve disk space
        optimize_png(repo, full_size)

        RepositoryTask.add(repo.repo_id, "screenshot.{}".format(name))

    return True


def optimize_png(repo: Repository, source: str):
    """Optimize PNG images from PhantomJS to reduce file sizes ~70% by reducing
    the color palette to 256 colors.

    :param repo: Repository to which the images belongs.
    :param source: Full path to the file being optimized.
    """
    if 'png' not in source:
        return False

    if not os.path.exists(source):
        log.warn('[%s] File does not exist: %s', repo.name, source)
        return False

    log.info("[%s] Optimizing PNG %s", repo.name, source)
    existing_size = os.path.getsize(source)

    optimized = Image.open(source).convert('P', palette=Image.ADAPTIVE)
    optimized.save(source)

    savings = existing_size - os.path.getsize(source)
    savings_pct = (savings / existing_size) * 100
    log.debug("[%s] Optimized PNG %s - %.2f%% saved (%dKB to %dkB)",
              repo.name, source, savings_pct, existing_size / 1024,
              (existing_size - savings) / 1024)

    return True


def generate_thumbnail(source: str, destination: str, dimensions: tuple):
    """Create a thumbnail from a PNG image.

    Pages without a default background color are saved by PhantomJS with a
    transparent background - these will be replaced with a white background.

    :param repo: The repository to generate thumbnails for.
    :param source: Full path to the full-size image.
    :param destination: Full path to save the thumbnail.
    :param dimensions: The (width, height) of the thumbnail.
    """
    log.info("Creating thumbnail from %s", source)
    img = Image.open(source)
    if img.mode == 'P':
        img = img.convert("RGBA")

    height = ceil(dimensions[0] * img.size[1] / img.size[0])
    img = img.resize((dimensions[0], height), Image.ANTIALIAS)

    try:
        bg = Image.new("RGB", dimensions, (255, 255, 255))
        bg.paste(img, (0, 0), img)
        bg.save(destination, "JPEG", optimize=True, quality=95)
    except Exception as e:
        log.error("Unable to paste image onto mask", e)
        return False

    return True


#: Regex to find a README in projects root folder
readme_re = re.compile('href="(/[^/]+/[^/]+/blob/[^/]+/([^/]+/)?readme(\.[^"]*)?)"',
                       re.IGNORECASE)


def _find_readme_url(name: str, age: int=86400*14):
    """Find the URL of the projects README."""
    log.debug("[%s] Finding README", name)

    content = httpcache.get('https://github.com/' + name, expiry=age)

    # Some repos have multiple README* files (such as php/php-src).
    links = readme_re.findall(content.decode('utf-8'))
    preferred = [x for x in links if x[1].lower() in ['.md', '.txt', '.rst']]
    if len(preferred):
        links = preferred
    if len(links):
        return "https://github.com" + links[0][0]
    return 'https://github.com/' + name
