from growser.cmdr import Command


class UpdateRepositoryMedia(Command):
    """Update all media elements for a single repository.

    Example::

        UpdateRepositoryMedia("pydata/pandas")

    :param name: Repository to update.
    """
    def __init__(self, name):
        self.name = name


class UpdateRepositoryScreenshot(Command):
    """Update the screenshot for a given URL & repository.

    Example::

        UpdateRepositoryScreenshot(
            "pydata/pandas",
            "http://pandas.pydata.org/",
            "static/github/fs/37be0d5d4911.hp.png"
        )

    :param name: Repository the URL belongs to
    :param url: URL to screenshot
    :param destination: Local path to save the image
    """
    def __init__(self, name: int, url: str, destination: str):
        self.name = name
        self.url = url
        self.destination = destination


class CreateResizedScreenshot(Command):
    """Resize a screenshot based on :attr:`~size`.

    Example::

        CreateResizedScreenshot("pydata/pandas", (400, 600)
            "static/github/fs/37be0d5d4911.hp.png",
            "static/github/md/37be0d5d4911.hp.jpg"
        )

    :param name: Name of the repository.
    :param size: Dimensions (width, height) to resize to.
    :param source: Source file used for the screenshot.
    :param destination: Path to save the resized image.
    """
    def __init__(self, name: str, size: tuple, source: str, destination: str):
        self.name = name
        self.size = size
        self.source = source
        self.destination = destination

    def __repr__(self):
        return '{}({}, {}, {}, {})'.format(
            self.__class__.__name__, self.name, self.source,
            self.destination, self.size)


class OptimizeImage(Command):
    """Reduce the color palette of an image to 256 to conserve disk space.

    Example::

        OptimizeImage("pydata/pandas", "static/github/fs/37be0d5d4911.hp.png")

    :param name: Name of the repository.
    :param source: Path to the file to optimize.
    """
    def __init__(self, name, source):
        self.name = name
        self.source = source


class CreateHeaderCollage(Command):
    """Create the homepage mast header.

    Example::

        cmd = CreatePageHeaderHomepageCollage(
            "static/github/md",
            "static/img/bg.50.png",
            10000,
            (50, 50),
            (1200, 300)
        )

    :param path: Path to the folder containing the images (screenshots) to use
                 when creating the header.
    :param destination: Path to save the header.
    :param num_repos: Filter images to only repos in the top-N.
    :param thumbnail_sizes: The max width/height when resizing thumbnails.
    :param header_sizes: The max width/height for the header.

    """
    def __init__(self, path: str, destination: str, num_repos: int,
                 thumbnail_sizes: tuple, header_sizes: tuple):
        self.path = path
        self.destination = destination
        self.num_repos = num_repos
        self.thumbnail_sizes = thumbnail_sizes
        self.header_sizes = header_sizes

