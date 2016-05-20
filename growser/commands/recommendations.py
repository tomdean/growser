from growser.cmdr import Command


class ExportRatingsToCSV(Command):
    def __init__(self, model: int):
        self.model = model


class ExecuteMahoutRecommender(Command):
    def __init__(self, model: int):
        self.model = model
