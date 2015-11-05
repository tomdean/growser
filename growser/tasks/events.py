import glob
import numpy as np
import pandas as pd

from growser.app import app


MIN_REPO_EVENTS = 75
MIN_LOGIN_EVENTS = 25


def process_events(path="data/events/events_*.gz"):
    """Process the entirety of the Github-Archive files.

    1. Assign unique IDs to logins & repositories
        a. Replace values for renamed repositories (e.g. node)
    2.

    :param path: Location of the gzip archives
    """
    app.logger.info("Processing GitHub Archive files")
    events = _get_events_dataframe(path, _get_renamed_repositories())
    repos = _distinct_values(events, 'repo', 'login')
    logins = _distinct_values(events, 'login', 'repo')

    # Filter to logins & repos with a minimum amount of activity
    min_repos = repos[repos['unique'] >= MIN_REPO_EVENTS]
    min_logins = logins[logins['unique'] >= MIN_LOGIN_EVENTS]

    # Update events to use numeric repo/login IDs along with any redirects
    app.logger.info("Filter events to repos & logins with min # of events")
    fields = ['type', 'repo_id', 'login_id', 'created_at_x']
    events = pd.merge(events, min_repos, on='repo')
    events = pd.merge(events, min_logins, on='login')[fields] \
        .rename(columns={'created_at_x': 'created_at'})

    # Eliminate dupes by grouping by type
    app.logger.info("Group per user/repo/type")
    per_type = events.groupby(['login_id', 'repo_id', 'type']) \
        .agg({'created_at': 'min'}) \
        .reset_index()

    # If a user has starred and forked a repo, rating will be 3
    app.logger.info("Group per user/repo")
    final = per_type.groupby(['login_id', 'repo_id']) \
        .agg({'type': 'sum', 'created_at': 'min'}) \
        .reset_index() \
        .rename(columns={'type': 'rating'})

    # Pre-sort so that most starred repositories are at the top
    repos.sort_values(['unique', 'repo'], ascending=False, inplace=True)

    app.logger.info("Writing results to CSV")
    repos.to_csv('data/csv/repos.csv', header=True, index=False)
    logins.to_csv('data/csv/logins.csv', header=True, index=False)
    final.to_csv('data/csv/ratings.csv', header=True, index=False)


def _distinct_values(df: pd.DataFrame, column: str, unique: str) -> pd.DataFrame:
    # Extract unique values from a `pandas.Series` sorted by earliest timestamp
    rv = df.groupby(column) \
        .agg({'created_at': [np.amin, np.count_nonzero],
              unique: lambda x: len(x.unique())}) \
        .reset_index()
    rv.columns = [column, 'unique', 'created_at', 'num_rows']
    return rv.sort_values(['created_at', column]) \
        .reset_index(drop=True) \
        .reset_index() \
        .rename(columns={'index': column + '_id'})


def _get_renamed_repositories() -> pd.DataFrame:
    rv = open("data/github/redirects.csv").read().split("\n")
    rv = [r for r in map(lambda x: x.split(","), rv) if len(r) == 2]
    return pd.DataFrame(rv, columns=["repo", "to"])


def _get_events_dataframe(path: str, renamed: pd.DataFrame) -> pd.DataFrame:
    files = glob.glob(path)
    fields = ['type', 'repo', 'login', 'created_at']
    dtypes = {'type': np.int,
              'repo': np.object,
              'login': np.object,
              'created_at': np.long}
    rv = []
    for file in files:
        app.logger.debug("Processing " + file)
        df = pd.read_csv(file, engine='c', usecols=fields, dtype=dtypes)
        df = pd.merge(df, renamed, how="left", on="repo")
        df['created_at'] = (df['created_at'] / 1000000).astype(np.int)
        df['repo'] = df['to'].fillna(df['repo'])
        df.drop(['to'], axis=1, inplace=True)
        rv.append(df)
    return pd.concat(rv)
