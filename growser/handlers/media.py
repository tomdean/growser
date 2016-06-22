from io import BytesIO
from itertools import product
import os
import math
import random
import re
from PIL import Image
import subprocess
from typing import List, Union

from celery import group
import numpy as np
import pandas as pd
from skimage.filters import sobel_h, sobel_v

from growser import httpcache
from growser.app import log
from growser.cmdr import DomainEvent, Handles
from growser.commands.media import (
    BatchUpdateRepositoryScreenshots,
    CalculateImageComplexityScore,
    CalculateImageComplexityScores,
    CreateHeaderCollage,
    CreateResizedScreenshot,
    OptimizeImage,
    UpdateRepositoryMedia,
    UpdateRepositoryScreenshot,
)
from growser.models import Rating, Repository, RepositoryTask
from growser.tasks import run_command


#: Command to execute PhantomJS for rendering screenshots
PHANTOM_JS_CMD = ["bin/phantomjs",
                  "--ignore-ssl-errors=true",
                  "bin/screenshot.js"]

#: The width & height for generated thumbnails.
THUMBNAIL_DIMENSIONS = (('ts', (150, 225)), ('md', (400, 600)))


class ImageCreated(DomainEvent):
    """New screenshot successfully created."""
    def __init__(self, name, path):
        self.name = name
        self.path = path


class ImageUpdated(ImageCreated):
    """Existing screen has been updated."""


class ImageOptimized(DomainEvent):
    def __init__(self, name, savings, savings_pct):
        self.name = name
        self.savings = savings
        self.savings_pct = savings_pct


class HeaderCollageCreated(DomainEvent):
    def __init__(self, path):
        self.path = path


class ImageComplexityScoresCalculated(DomainEvent):
    """Image complexity scores saved to `destination`."""
    def __init__(self, destination):
        self.destination = destination


#: Regex to find a README in projects root folder
readme_re = \
    re.compile('href="(/[^/]+/[^/]+/blob/[^/]+/([^/]+/)?readme(\.[^"]*)?)"',
               re.IGNORECASE)


class MediaManager:  # pragma: no cover
    """Too lazy to inject this into the handlers. Cheating by using globals."""
    @staticmethod
    def open(path: str) -> Image.Image:
        return Image.open(path)

    @staticmethod
    def new(mode: str, size: tuple, color: tuple = None) -> Image.Image:
        return Image.new(mode, size, color)

    @staticmethod
    def exists(path):
        return os.path.exists(path)

    @staticmethod
    def size(path):
        return os.path.getsize(path)

    @staticmethod
    def walk(path):
        filenames = (os.path.join(path, f) for f in os.listdir(path))
        return zip(filenames, map(os.path.getsize, filenames))

Images = MediaManager


class UpdateRepositoryMediaHandler(Handles[UpdateRepositoryMedia]):
    def handle(self, cmd: UpdateRepositoryMedia):
        """Update the screenshots for the GitHub readme & project homepage."""
        from growser.models import Repository

        repo = Repository.query.filter(Repository.name == cmd.name).first()

        urls = []
        if repo.homepage:
            urls.append(("hp", repo.homepage))

        readme_url = self._find_readme_url(repo.name)
        if readme_url:
            urls.append(('readme', readme_url))

        if not len(urls):
            return False

        def image_path(f, h, p, e):
            return "static/github/{folder}/{hashid}.{page}.{ext}" \
                .format(hashid=h, page=p, folder=f, ext=e)

        for name, url in urls:
            path = image_path("fs", repo.hashid, name, "png")
            yield UpdateRepositoryScreenshot(repo.repo_id, repo.name, url, path)

            for folder, size in THUMBNAIL_DIMENSIONS:
                source = image_path(folder, repo.hashid, name, "jpg")
                yield CreateResizedScreenshot(repo.name, size, path, source)

            yield OptimizeImage(repo.name, path)

    @staticmethod
    def _find_readme_url(name: str, age: int=86400*14) -> str:
        """Find the URL of the projects README."""
        content = httpcache.get('https://github.com/' + name, expires=age)

        # Some repos have multiple README* files (such as php/php-src).
        links = readme_re.findall(content.decode('utf-8'))
        extensions = ['.md', '.txt', '.rst']
        preferred = [x for x in links if x[1].lower() in extensions]
        if len(preferred):
            links = preferred
        if len(links):
            return "https://github.com" + links[0][0]
        return 'https://github.com/' + name


class UpdateRepositoryScreenshotHandler(Handles[UpdateRepositoryScreenshot]):
    def handle(self, cmd: UpdateRepositoryScreenshot) \
            -> Union[ImageCreated, ImageUpdated]:
        """Invokes the PhantomJS binary to render a screenshot."""
        updated = Images.exists(cmd.destination)

        subprocess.call(PHANTOM_JS_CMD + [cmd.url, cmd.destination],
                        stdout=subprocess.DEVNULL)

        # @todo Move this to an event listener
        RepositoryTask.add(cmd.repo_id, 'screenshot.hp')

        cls = ImageUpdated if updated else ImageCreated
        return cls(cmd.name, cmd.destination)

    def batch(self, cmd: BatchUpdateRepositoryScreenshots):
        """Convenience handler to batch update repository screenshots."""
        repos = get_repositories(cmd.limit, cmd.task_window,
                                 cmd.rating_window, cmd.min_events)
        for repo in repos:
            run_command.delay(UpdateRepositoryMedia(repo.repo_id, repo.name))


class CreateResizedScreenshotHandler(Handles[CreateResizedScreenshot]):
    def handle(self, cmd: CreateResizedScreenshot) -> ImageCreated:
        img = Images.open(cmd.source)

        if img.mode == 'P':
            img = img.convert("RGB")

        height = math.ceil(cmd.size[0] * img.size[1] / img.size[0])
        img = img.resize((cmd.size[0], height), Image.ANTIALIAS)

        bg = Images.new("RGB", cmd.size, (255, 255, 255))
        bg.paste(img, (0, 0), img)
        bg.save(cmd.destination, "JPEG", optimize=True, quality=95)

        return ImageCreated(cmd.name, cmd.destination)


class OptimizeImageHandler(Handles[OptimizeImage]):
    def handle(self, cmd: OptimizeImage) -> ImageOptimized:
        """Rather than dispose of the full-size screenshots, reduce their
        color palette to 256 colors and file size by ~70%."""
        if not Images.exists(cmd.source):
            raise FileNotFoundError(cmd.source)

        size = Images.size(cmd.source)
        img = Images.open(cmd.source).convert('P', palette=Image.ADAPTIVE)
        img.save(cmd.source)

        return ImageOptimized(cmd.name, size, Images.size(cmd.source))


class CreateHeaderCollageHandler(Handles[CreateHeaderCollage]):
    def handle(self, cmd: CreateHeaderCollage) -> HeaderCollageCreated:
        """Create a grid of screenshots to use for the homepage."""
        size = (cmd.thumbnail_width, cmd.thumbnail_width * 2)

        # Load the first image to determine our width/height
        images = self._filter_files(cmd.path)
        first = self._get_resized_thumbnails(images[:1], size)[0]

        width, height = first.size
        x, y = cmd.grid_size

        # Pick a sample of the images based on the size of the grid
        images = random.sample(images, cmd.grid_size[0] * cmd.grid_size[1])
        images = self._get_resized_thumbnails(images, first.size)

        img = Images.new("RGB", (width * x, height * y))
        for row, col in product(range(y), range(x)):
            img.paste(images[row * x + col], (col * width, row * height))
        img.save(cmd.destination)

        return HeaderCollageCreated(cmd.destination)

    def _filter_files(self, path) -> List[str]:
        df = pd.read_csv(path)
        df = df.sort_values('compression', ascending=False)

        # Remove single-color images and those less than the mean
        df = df[(df['edges'] > 0)]
        df = df[df['edges'] >= df['edges'].mean()]

        # Exclude images where top 3 colors >= 90% of total color distribution
        def color_pct(x):
            return np.sum(np.sort(x[::, 0] / np.sum(x[::, 0]))[-3:])

        df['histogram'] = df['histogram'].apply(eval).apply(np.asarray)
        df = df[df['histogram'].map(color_pct) <= 0.93]

        return df['filename'].values.tolist()

    def _get_resized_thumbnails(self, filenames, size) -> List[Image.Image]:
        """Return a list of resized thumbnails."""
        rv = []
        for filename in filenames:
            image = Images.open(filename)
            image.thumbnail(size, Image.ANTIALIAS)
            rv.append(image)
        return rv


class CalculateImageComplexityHandler(Handles[CalculateImageComplexityScores]):
    def handle(self, cmd: CalculateImageComplexityScores) \
            -> ImageComplexityScoresCalculated:
        """Use Celery to calculate the image complexity scores concurrently."""
        filenames = self._get_filenames(cmd.path, cmd.pattern)

        rv = []

        batch_size = 100
        log.info("Processing {} images".format(len(filenames)))
        for batch in batched(filenames, batch_size):
            jobs = map(run_command.s, map(CalculateImageComplexityScore, batch))
            result = group(jobs).apply_async()
            rv += result.get(interval=1)
        self._to_csv(cmd.destination, rv)

        return ImageComplexityScoresCalculated(cmd.destination)

    def score_image(self, cmd: CalculateImageComplexityScore):
        from growser.media import score_image
        return [cmd.filename, *score_image(Images.open(cmd.filename))]

    def _get_filenames(self, path, pattern) -> List[str]:
        """Return a list of all files in `path` matching `pattern`."""
        filenames = []
        for filename in os.listdir(path):
            if pattern and pattern not in filename:
                continue
            filenames.append(os.path.join(path, filename))
        return filenames

    def _to_csv(self, destination, images):
        columns = ['filename', 'compression', 'edges', 'histogram']
        pd.DataFrame(images, columns=columns).to_csv(destination, index=False)


def get_repositories(limit: int, task_days: int,
                     rating_days: int, min_events: int):
    from datetime import date, timedelta
    from sqlalchemy import and_, func, exists

    from growser.app import db

    tasks = and_(
        RepositoryTask.name.like('screenshot.%'),
        RepositoryTask.repo_id == Rating.repo_id,
        RepositoryTask.created_at >= func.now() - timedelta(days=task_days)
    )

    # Only update repositories that have recent activity
    query = db.session.query(Rating.repo_id, func.count(1).label('num_events')) \
        .filter(~exists().where(tasks)) \
        .filter(Rating.created_at >= date.today() - timedelta(days=rating_days)) \
        .group_by(Rating.repo_id) \
        .having(func.count(1) >= min_events) \
        .order_by(func.count(1).desc())

    # Return (repo_id, name, num_events)
    popular = query.subquery()
    candidates = db.session.query(Repository.repo_id,
                                  Repository.name,
                                  popular.columns.num_events) \
        .filter(Repository.status == 1) \
        .join(popular, popular.columns.repo_id == Repository.repo_id)

    return candidates.limit(limit).all()


def batched(l, n):
    for i in range(0, len(l), n):
        yield l[i:i+n]


def get_compressed_size(image: Image, quality) -> float:
    """Find the size of an image after JPEG compression.

    :param image: Image to compress.
    :param quality: JPEG compression quality (1-100).
    """
    with BytesIO() as fh:
        image.save(fh, 'JPEG', quality=quality)
        size = len(fh.getvalue())
    return size


def get_compression_ratio(image: Image) -> float:
    """Return the lossless compression ratio for an image."""
    lossless = get_compressed_size(image, 100)
    compressed = get_compressed_size(image, 75)
    return compressed / lossless


def get_spatial_information_score(image: Image) -> float:
    """Use Sobel filters to find the edge energy of an image.

    .. math::
        SI_r = \sqrt{S_v^2 + S_h^2}

        SI_{mean} = \frac{1}{P}\sum{SI_r,}

    Where :math:`SI_r` is the spatial energy for each pixel and :math:`P` the
    number of pixels.

    .. seealso:: http://vintage.winklerbros.net/Publications/qomex2013si.pdf
    """
    img = np.asarray(image)
    num_pixels = img.shape[0] * img.shape[1]
    energy = np.sum(np.sqrt(sobel_v(img) ** 2 + sobel_h(img) ** 2))
    return energy / num_pixels


def score_image(image: Image):
    """Return the JPEG compression ratio and spatial complexity of an image.

    Gray-scaling images prevents color from impacting compression performance.

    :param image: Pillow Image instance.
    """
    if image.mode != 'L':
        image = image.convert('L')
    cr = get_compression_ratio(image)
    si = get_spatial_information_score(image)
    hg = np.histogram(image, bins=6)
    return cr, si, tuple(zip(*hg))
