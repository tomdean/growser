from itertools import chain, product
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

from growser import httpcache
from growser.cmdr import DomainEvent, Handles
from growser.commands.media import (
    CalculateImageComplexityScores,
    CreateHeaderCollage,
    CreateResizedScreenshot,
    OptimizeImage,
    UpdateRepositoryMedia,
    UpdateRepositoryScreenshot,
)
from growser.models import RepositoryTask
from growser.tasks import score_images


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
    def handle(self, cmd: UpdateRepositoryMedia) -> DomainEvent:
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
    def _find_readme_url(name: str, age: int=86400*14):
        """Find the URL of the projects README."""
        content = httpcache.get('https://github.com/' + name, expiry=age)

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

        self.subprocess(PHANTOM_JS_CMD + [cmd.url, cmd.destination])

        # @todo Move this to an event listener
        RepositoryTask.add(cmd.repo_id, 'screenshot.hp')

        if updated:
            return ImageUpdated(cmd.name, cmd.destination)
        return ImageCreated(cmd.name, cmd.destination)

    def subprocess(self, cmd):
        return subprocess.call(cmd, stdout=subprocess.DEVNULL)


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

    def _filter_files(self, path):
        df = pd.read_csv(path)
        df = df.sort_values('compression', ascending=False)

        # Remove single-color images and those less than the mean
        df = df[(df['edges'] > 0)]
        df = df[df['edges'] >= df['edges'].mean()]

        # Exclude images where top 3 colors >= 90% of total color distribution
        def color_pct(x):
            return np.sum(np.sort(x[::, 0] / np.sum(x[::, 0]))[-3:])

        df['histogram'] = df['histogram'].apply(eval).apply(np.asarray)
        df = df[df['histogram'].map(color_pct) <= 0.915]

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
    def handle(self, cmd: CalculateImageComplexityScores):
        """Use Celery to calculate the image complexity scores concurrently."""
        filenames = self.get_filenames(cmd.path, cmd.pattern)

        def batched(l, n):
            for i in range(0, len(l), n):
                yield l[i:i+n]

        batch_size = 100
        tasks = map(score_images.s, batched(filenames, batch_size))
        result = group(tasks).apply_async()
        images = result.get(interval=1)

        self.to_csv(cmd.destination, list(chain(*images)))

        return ImageComplexityScoresCalculated(cmd.destination)

    def get_filenames(self, path, pattern):
        filenames = []
        for filename in os.listdir(path):
            if pattern and pattern not in filename:
                continue
            filenames.append(os.path.join(path, filename))
        return filenames

    def to_csv(self, destination, images):
        df = pd.DataFrame(images, columns=['filename', 'compression',
                                           'edges', 'histogram'])
        df.to_csv(destination, index=False)
