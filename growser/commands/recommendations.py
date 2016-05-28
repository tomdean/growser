
from growser.cmdr import Command


class ExportRatingsToCSV(Command):
    def __init__(self, model: int):
        """Export ratings to a CSV file used by the recommender."""
        self.model = model


class ExecuteMahoutRecommender(Command):
    def __init__(self, model: int):
        """Execute the Mahout recommender."""
        self.model = model
