from collections import namedtuple
import csv
import gzip
from os.path import abspath, join
import subprocess

from growser.app import db, log
from growser.db import from_sqlalchemy_table
from growser.models import Recommendation

from growser.cmdr import DomainEvent, Handles
from growser.commands.recommendations import (
    ExecuteMahoutRecommender,
    ExportRatingsToCSV
)

RecModel = namedtuple('RecModel', ['id', 'source', 'destination', 'sql'])

SQL_PATH = "deploy/etl/sql/recs"
RATINGS_PATH = "data/ratings"
EXPORT_PATH = "data/recs"

MODELS = {
    1: RecModel(1, 'all.csv', 'mahout.all.csv.gz', 'ratings.all.sql'),
    2: RecModel(2, 'year.csv', 'mahout.year.csv.gz', 'ratings.year.sql'),
    3: RecModel(3, '120.csv', 'mahout.120.csv.gz', 'ratings.120.sql')
}


class RatingsExported(DomainEvent):
    def __init__(self, model):
        self.model = model


class RecommendationsUpdated(DomainEvent):
    def __init__(self, model: int, num_results: int):
        self.model = model
        self.num_results = num_results


class ExportRatingsToCSVHandler(Handles[ExportRatingsToCSV]):
    def handle(self, cmd: ExportRatingsToCSV):
        model = MODELS.get(cmd.model)
        sql = open(join(SQL_PATH, model.sql)).read()
        db.engine.execute(sql)
        return RatingsExported(cmd.model)


class ExecuteMahoutRecommenderHandler(Handles[ExecuteMahoutRecommender]):
    def handle(self, cmd: ExecuteMahoutRecommender):
        model = MODELS.get(cmd.model)

        source = abspath(join(RATINGS_PATH, model.source))
        destination = abspath(join(EXPORT_PATH, model.destination))

        log.info('Running Mahout')
        run = ["mvn", "exec:java", "-DbatchSize=100",
               "-DmodelID={}".format(model.id),
               "-Dsrc=" + source,
               "-Dout=" + destination]
        subprocess.call(run, cwd="../growser-mahout/")

        Recommendation.query.filter(
            Recommendation.model_id == model.id).delete()

        columns = ['model_id', 'repo_id', 'recommended_repo_id', 'score']
        batch = from_sqlalchemy_table(
            Recommendation.__table__, from_csv(destination), columns)

        for rows in batch.batch_execute(db.engine.raw_connection):
            log.info("Batch complete: {}".format(rows))

        return RecommendationsUpdated(model.id, batch)


def from_csv(path):
    file = gzip.open(path, 'rt') if path.endswith('gz') else open(path, 'rt')
    return csv.reader(file)
