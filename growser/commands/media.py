from growser.cmdr import Command


class UpdateRepositoryMedia(Command):
    """Update all media elements for a single repository.

    Example::

        UpdateRepositoryMedia("pydata/pandas")

    :param name: Repository to update.
    """
    def __init__(self, repo_id, name):
        self.repo_id = repo_id
        self.name = name

    def __repr__(self):
        return "<{} name={}>".format(self.__class__.__qualname__, self.name)


class UpdateRepositoryScreenshot(Command):
    """Update the screenshot for a given URL & repository.

    Example::

        UpdateRepositoryScreenshot(
            "pydata/pandas",
            "http://pandas.pydata.org/",
            "static/github/fs/37be0d5d4911.hp.png"
        )

    :param repo_id: Internal ID of the repository.
    :param name: Repository the URL belongs to.
    :param url: URL to screenshot.
    :param destination: Local path to save the image.
    """
    def __init__(self, repo_id: int, name: str, url: str, destination: str):
        self.repo_id = repo_id
        self.name = name
        self.url = url
        self.destination = destination

    def __repr__(self):
        return "<{} name={}>".format(self.__class__.__qualname__, self.name)


class BatchUpdateRepositoryScreenshots(Command):
    """Update screenshots for multiple repositories.

    Example::

        BatchUpdateRepositoryScreenshots(1000, 180, 30)

    :param limit: Total number of repositories to update.
    :param rating_window: Sort repos by most ratings within this time frame
    :param task_window: Exclude repos that have already been updated in
                        `task_window` prior days.
    :param min_events: Minimum events in `rating_window`.
    """
    def __init__(self, limit: int, rating_window: int=90,
                 task_window: int=30, min_events: int=100):
        self.limit = limit
        self.rating_window = rating_window
        self.task_window = task_window
        self.min_events = min_events


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
        return '<{} name={} size={}>'.format(self.__class__.__qualname__,
                                             self.name, self.size)


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
    """Create NxM grid of screenshots.

    Example::

        cmd = CreateHeaderCollage(
            "data/csv/collage.csv",
            "static/img/bg.50.png",
            50,
            (24, 5)
        )

    :param path: Path to a file containing a list of files to be used.
    :param destination: Path to save the header.
    :param thumbnail_width: Resize thumbnails to this width (pixels).
    :param grid_size: Size of the grid to create
    """
    def __init__(self, path: str, destination: str,  thumbnail_width: int,
                 grid_size: tuple):
        self.path = path
        self.destination = destination
        self.thumbnail_width = thumbnail_width
        self.grid_size = grid_size


class CalculateImageComplexityScores(Command):
    """Calculate a complexity score for all images in a folder, saving the
    results to a CSV file::

        cmd = CalculateImageComplexityScores(
            "static/github/md/",
            "data/csv/screenshots.csv",
            ".hp."
        )

    :param path: Directory of images to calculate scores for.
    :param destination: Destination to save CSV results to.
    :param pattern: (Optional) Filter to images containing this value.
    """
    def __init__(self, path: str, destination: str, pattern: str=None):
        self.path = path
        self.destination = destination
        self.pattern = pattern


class CalculateImageComplexityScore(Command):
    def __init__(self, filename):
        self.filename = filename
