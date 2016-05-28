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
    def __init__(self, name: str, url: str, destination: str):
        self.name = name
        self.url = url
        self.destination = destination


class CreateResizedScreenshot(Command):
    """Resize a screenshot.

    Example::

        CreateResizedScreenshot(
            "pydata/pandas",
            (400, 600),
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

        cmd = CreateHeaderCollageHandler(
            "data/csv/collage.csv",
            "static/img/bg.50.png",
            (50, 50),
            (1200, 300)
        )

    :param path: Path to a file containing a list of files to be used.
    :param destination: Path to save the header.
    :param thumbnail_sizes: The max width/height when resizing thumbnails.
    :param header_sizes: The max width/height for the header.
    """
    def __init__(self, path: str, destination: str,  thumbnail_sizes: tuple,
                 header_sizes: tuple):
        self.path = path
        self.destination = destination
        self.thumbnail_sizes = thumbnail_sizes
        self.header_sizes = header_sizes


class CalculateImageComplexityScores(Command):
    """Calculate a complexity score of all images in a folder, and save the
    results to a CSV file::

        cmd = CalculateImageComplexityScores(
            "static/github/md/",
            "data/csv/screenshots.csv",
            ".hp."
        )

    :param path: Directory of images to calculate scores for.
    :param destination: File to save results to.
    :param pattern: Only include files that contain this value.
    """
    def __init__(self, path: str, destination: str, pattern: str=None):
        self.path = path
        self.destination = destination
        self.pattern = pattern
