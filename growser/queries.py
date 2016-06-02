from growser.cmdr import Query


class FindProject(Query):
    """Return a single repository by name."""
    def __init__(self, name: str):
        self.name = name


class FindRecommendations(Query):
    """Return recommendations based on a given repository & model."""
    def __init__(self, repo_id: int, model: int, limit: int):
        self.repo_id = repo_id
        self.model = model
        self.limit = limit


class FindCurrentRankings(Query):
    """Current rankings, across all time dimensions, for a single repository."""
    def __init__(self, repo_id):
        self.repo_id = repo_id


class FindTopLanguages(Query):
    """Most popular languages in the past year based on # of stars & forks."""
    def __init__(self, limit: int=25):
        self.limit = limit


class FindReleases(Query):
    """Recent releases for a repository."""
    def __init__(self, repo_id: int, limit: int, offset: int):
        self.repo_id = repo_id
        self.limit = limit
        self.offset = offset
