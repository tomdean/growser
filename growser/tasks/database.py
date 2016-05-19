import glob
import gzip
import json


import numpy as np
import pandas as pd
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.schema import Index

from growser.app import db, log
from growser.models import Rating, Repository, Recommendation


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


def pre_warm_tables():
    """Pre-warm tables to improve cold start performance."""
    for table in db.metadata.tables:
        db.engine.execute("SELECT pg_prewarm(%s)", table)


def merge_repository_data_sources():
    """Merge GitHub API repository data with events data."""
    log.info("Merging repository data sources")
    df1 = pd.read_csv('data/csv/repos.csv')

    df1['created_at'] = pd.to_datetime(df1['created_at'])
    df1['owner'] = df1['name'].str.split("/").str.get(0)
    df1 = df1[df1['owner'] != ''].copy()

    # GitHub API data
    df2 = get_github_api_results("data/github-api/repos/*.json.gz")
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
    df.sort_values("num_stars", ascending=False, inplace=True)

    # Keep these fields and in this order
    fields = ['repo_id', 'name', 'owner', 'organization', 'language',
              'description', 'homepage', 'num_stars', 'num_forks',
              'num_watchers', 'updated_at', 'created_at']

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
            return ''
        url = url.strip()
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
            'homepage': homepage(content['homepage']),
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
