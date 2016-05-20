import os
from collections import defaultdict
import math
from random import shuffle
import re
from PIL import Image
import subprocess

from growser import httpcache
from growser.commands.media import (
    CreateHeaderCollage,
    CreateResizedScreenshot,
    OptimizeImage,
    UpdateRepositoryMedia,
    UpdateRepositoryScreenshot,
)
from growser.cmdr import DomainEvent, Handles

#: Command to execute PhantomJS for rendering screenshots
PHANTOM_JS_CMD = ["bin/phantomjs",
                  "--ignore-ssl-errors=true",
                  "bin/screenshot.js"]

#: The width & height for generated thumbnails.
THUMBNAIL_DIMENSIONS = (('ts', (150, 225)), ('md', (400, 600)))


class ImageCreated(DomainEvent):
    def __init__(self, name, path):
        self.name = name
        self.path = path


class ImageUpdated(ImageCreated):
    pass


class ImageOptimized(DomainEvent):
    def __init__(self, name, savings, savings_pct):
        self.name = name
        self.savings = savings
        self.savings_pct = savings_pct


class HeaderCollageCreated(DomainEvent):
    def __init__(self, path):
        self.path = path


#: Regex to find a README in projects root folder
readme_re = \
    re.compile('href="(/[^/]+/[^/]+/blob/[^/]+/([^/]+/)?readme(\.[^"]*)?)"',
               re.IGNORECASE)


class UpdateRepositoryMediaHandler(Handles[UpdateRepositoryMedia]):
    def handle(self, cmd: UpdateRepositoryMedia) -> DomainEvent:
        """Update the screenshots for the GitHub readme & project homepage.

        @todo: This is more like a "command coordinator" or CQRS saga.
        """
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
            destination = image_path("fs", repo.hashid, name, "png")
            yield UpdateRepositoryScreenshot(repo.name, url, destination)

            for folder, size in THUMBNAIL_DIMENSIONS:
                source = image_path(folder, repo.hashid, name, "jpg")
                yield CreateResizedScreenshot(
                    repo.name, size, destination, source)

            yield OptimizeImage(repo.name, destination)

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
    def handle(self, cmd: UpdateRepositoryScreenshot):
        """Invokes the PhantomJS binary to render a URLs' screenshot."""
        existed = os.path.exists(cmd.destination)
        subprocess.call(PHANTOM_JS_CMD + [cmd.url, cmd.destination])
        klass = ImageUpdated if existed else ImageCreated
        yield klass(cmd.name, cmd.destination)


class CreateResizedScreenshotHandler(Handles[CreateResizedScreenshot]):
    def handle(self, cmd: CreateResizedScreenshot) -> ImageCreated:
        img = Image.open(cmd.source)
        if img.mode == 'P':
            img = img.convert("RGB")

        height = math.ceil(cmd.size[0] * img.size[1] / img.size[0])
        img = img.resize((cmd.size[0], height), Image.ANTIALIAS)

        bg = Image.new("RGB", cmd.size, (255, 255, 255))
        bg.paste(img, (0, 0), img)
        bg.save(cmd.destination, "JPEG", optimize=True, quality=95)

        yield ImageCreated(cmd.name, cmd.destination)


class OptimizeImageHandler(Handles[OptimizeImage]):
    def handle(self, cmd: OptimizeImage) -> ImageOptimized:
        """Rather than dispose of the full-size screenshots, reduce their
        color palette to 256 colors and file size by ~70%."""
        if 'png' not in cmd.source:
            return False

        if not os.path.exists(cmd.source):
            return False

        existing_size = os.path.getsize(cmd.source)
        optimized = Image.open(cmd.source).convert('P', palette=Image.ADAPTIVE)
        optimized.save(cmd.source)

        savings = existing_size - os.path.getsize(cmd.source)
        savings_pct = (savings / existing_size) * 100

        yield ImageOptimized(cmd.name, savings, savings_pct)


class CreateHeaderCollageHandler(Handles[CreateHeaderCollage]):
    def handle(self, cmd: CreateHeaderCollage) -> HeaderCollageCreated:
        """Create a grid of screenshots to use for the homepage."""
        images = self._get_resized_thumbnails(cmd)

        width, height = images[0].size[0], images[0].size[1]
        max_width, max_height = cmd.header_sizes[0], cmd.header_sizes[1]

        x = math.ceil(max_width / width)
        y = math.ceil(max_height / height)

        shuffle(images)
        img = Image.new("RGB", (width * x, height * y))
        for row in range(y):
            for col in range(x):
                img.paste(images[row * x + col], (col * width, row * height))
        img.save(cmd.destination)

        yield HeaderCollageCreated(cmd.destination)

    def _get_resized_thumbnails(self, cmd: CreateHeaderCollage):
        """Return a list of resized thumbnails."""
        files = self._get_filtered_files(cmd)

        thumbnails = []
        for hashid, size in files:
            img = Image.open(os.path.join(cmd.path, "{}.hp.jpg".format(hashid)))
            img.thumbnail(cmd.thumbnail_sizes)
            thumbnails.append(img)

        min_colors = int((thumbnails[0].size[0] * thumbnails[0].size[1]) * 0.6)
        filtered = []
        for img in thumbnails:
            colors = img.getcolors(min_colors)
            if not colors:
                filtered.append(img)

        return filtered

    def _get_filtered_files(self, cmd: CreateHeaderCollage):
        from growser.models import Repository

        """Return only thumbnails for the most popular repositories."""
        top_repos = Repository.query.filter(Repository.status == 1) \
            .filter(Repository.homepage != '') \
            .order_by(Repository.num_events.desc()) \
            .limit(cmd.num_repos).all()

        top_repos = [r.hashid for r in top_repos]
        meta = self._get_image_sizes(cmd.path)
        return [(h, _) for h, _ in meta if h in top_repos]

    @staticmethod
    def _get_image_sizes(path):
        """Returns a list of tuples (hash, file size) of images in `path`."""
        meta = defaultdict(dict)
        for filename in [f for f in os.listdir(path) if ".hp." in f]:
            hashid = filename.split('.')[0]
            meta[hashid]['size'] = os.stat(os.path.join(path, filename)).st_size

        return sorted([(idx, m['size']) for idx, m in meta.items()],
                      key=lambda x: x[1], reverse=True)
