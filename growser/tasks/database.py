import glob
import gzip
import json


import numpy as np
import pandas as pd
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.schema import Index

from growser.app import db, log
from growser.db import BulkInsertCSV
from growser.models import Login, MonthlyRanking, MonthlyRankingByLanguage, \
    Rating, Repository, Recommendation, RecommendationModel, WeeklyRanking, \
    WeeklyRankingByLanguage


def initialize_database_schema():
    """Drop & recreate the SQL schema."""
    log.info("Drop and recreate all model tables")
    db.drop_all()
    db.create_all()


def initialize_database_data_from_csv():
    """Populate the initial database from previously generated CSV sources."""
    # Data from GitHub Archive & static data
    jobs = [BulkInsertCSV(Login.__table__, "data/csv/logins.csv"),
            BulkInsertCSV(Repository.__table__, "data/csv/repositories.csv"),
            BulkInsertCSV(Rating.__table__, "data/csv/ratings.datetime.csv"),
            BulkInsertCSV(RecommendationModel.__table__, "data/models.csv")]

    # Recommendations
    columns = ['model_id', 'repo_id', 'recommended_repo_id', 'score']
    for filename in glob.glob('data/recommendations/*/*.gz'):
        jobs.append(BulkInsertCSV(Recommendation.__table__,
                                  filename, header=False, columns=columns))

    log.info("Bulk inserting CSV files into tables")
    for job in jobs:
        log.info("Running %s", job)
        job.execute(db.engine)


def create_secondary_indexes():
    """Create secondary indexes to improve performance while browsing."""
    log.info("Creating secondary indexes")

    def create(name, *args):
        log.debug("Creating %s", name)
        index = Index(name, *args, quote=False)
        try:
            index.drop(db.engine)
        except ProgrammingError:
            pass
        index.create(db.engine)

    create("IX_repository_language", Repository.language)
    create("IX_rating_repo", Rating.repo_id, Rating.created_at)
    create("IX_rating_created_at", Rating.created_at, Rating.repo_id)
    create("IX_recommendation_model_repo",
           Recommendation.model_id, Recommendation.repo_id)

    def ranking_index(name, model):
        create(name, model.repo_id, model.date)

    ranking_index("IX_weekly_ranking_repo", WeeklyRanking)
    ranking_index("IX_weekly_ranking_by_lang_repo", WeeklyRankingByLanguage)
    ranking_index("IX_monthly_ranking_repo", MonthlyRanking)
    ranking_index("IX_monthly_ranking_by_lang_repo", MonthlyRankingByLanguage)


def pre_warm_tables():
    """Pre-warm tables to improve cold start performance."""
    for table in db.metadata.tables:
        db.engine.execute("SELECT pg_prewarm(%s)", table)


def merge_repository_data_sources():
    """Merge GitHub API repository data with events data."""
    # Data from processing events gives us local ID
    df1 = pd.read_csv('data/csv/repos.csv')
    df1['created_at'] = pd.to_datetime(df1['created_at'], unit='s')
    df1['owner'] = df1['name'].str.split("/").str.get(0)
    df1 = df1[df1['owner'] != ''].copy()

    # GitHub API data
    df2 = get_github_api_results("data/github-api/*.json.gz")
    df2['created_at_alt'] = pd.to_datetime(df2['created_at_alt'])
    df2['updated_at'] = pd.to_datetime(df2['updated_at'], unit='s')

    log.info("Merging data")
    df = pd.merge(df1, df2, on="name", how="left")
    df['created_at'] = df['created_at_alt'].fillna(df['created_at'])
    df['updated_at'] = df['updated_at'].fillna(df['created_at'])
    df['num_stars'] = df['num_stars'].fillna(0).astype(np.int)
    df['num_forks'] = df['num_forks'].fillna(0).astype(np.int)
    df['num_watchers'] = df['num_watchers'].fillna(0).astype(np.int)

    # Pre-sort so that most popular repositories come first
    df.sort_values("num_events", ascending=False, inplace=True)

    # Keep these fields and in this order
    fields = ['repo_id', 'name', 'owner', 'organization', 'language',
              'description', 'num_events', 'num_unique', 'num_stars',
              'num_forks', 'num_watchers', 'updated_at', 'created_at']

    log.info("Saving...")
    df[fields].to_csv("data/csv/repositories.csv", index=False)


def get_github_api_results(path: str) -> pd.DataFrame:
    """Parse the JSON fetched from the GitHub API."""
    exists = {}
    files = glob.glob(path)

    def to_org(r):
        return r['organization']['login'] if 'organization' in r else ''

    def homepage(url):
        if not url:
            return None
        if 'http' not in url:
            return 'http://' + url
        return url

    rv = []
    log.info("Processing {} JSON files".format(len(files)))
    for filename in files:
        content = json.loads(gzip.open(filename).read().decode("UTF-8"))
        # Not found or rate limit error edge case
        if 'owner' not in content or content['full_name'] in exists:
            continue
        exists[content['full_name']] = True
        rv.append({
            'name': content['full_name'],
            'homepage': homepage(content['homepage'].strip()),
            'organization': to_org(content),
            'language': content['language'],
            'description': (content['description'] or "").replace("\n", ""),
            'num_stars': int(content['watchers']),
            'num_forks': int(content['forks']),
            'num_watchers': int(content['subscribers_count']),
            'updated_at': content['updated_at'],
            'created_at_alt': content['created_at']
        })
    return pd.DataFrame(rv)
