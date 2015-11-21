import glob
import gzip
import json

import numpy as np
import pandas as pd

from growser.app import app


def merge_repository_data_sources():
    """Merge GitHub API repository data along with data generated while
    processing events."""
    results = []
    exists = {}
    files = glob.glob("data/github-api/*.json.gz")

    def to_org(r):
        return r['organization']['login'] if 'organization' in r else ''

    app.logger.info("Processing {} JSON files".format(len(files)))
    for filename in files:
        content = json.loads(gzip.open(filename).read().decode("UTF-8"))
        if 'owner' not in content or content['full_name'] in exists:
            continue
        exists[content['full_name']] = True
        results.append({
            'name': content['full_name'],
            'organization': to_org(content),
            'language': content['language'],
            'description': (content['description'] or "").replace("\n", ""),
            'num_stars': int(content['watchers']),
            'num_forks': int(content['forks']),
            'num_watchers': int(content['subscribers_count']),
            'updated_at': content['updated_at'],
            'created_at_alt': content['created_at']
        })

    app.logger.info("Merging files")

    # Data generated during event processing
    df1 = pd.read_csv('data/csv/repos.csv')
    df1['created_at'] = pd.to_datetime(df1['created_at'], unit='s')
    df1['owner'] = df1['name'].str.split("/").str.get(0)
    df1 = df1[df1['owner'] != ''].copy()

    # GitHub API data
    df2 = pd.DataFrame(results)
    df2['created_at_alt'] = pd.to_datetime(df2['created_at_alt'])
    df2['updated_at'] = pd.to_datetime(df2['updated_at'], unit='s')

    df = pd.merge(df1, df2, on="name", how="left")
    df['created_at'] = df['created_at_alt'].fillna(df['created_at'])
    df['updated_at'] = df['updated_at'].fillna(df['created_at'])
    df['num_stars'] = df['num_stars'].fillna(0).astype(np.int)
    df['num_forks'] = df['num_forks'].fillna(0).astype(np.int)
    df['num_watchers'] = df['num_watchers'].fillna(0).astype(np.int)

    app.logger.info("Saving...")

    # Pre-sort so that most popular repositories come first
    df.sort_values("num_events", ascending=False, inplace=True)

    # Retain these fields and in this order
    fields = ['repo_id', 'name', 'owner', 'organization', 'language',
              'description', 'num_events', 'num_unique', 'num_stars',
              'num_forks', 'num_watchers', 'updated_at', 'created_at']
    df[fields].to_csv("data/csv/repositories.csv", index=False)
